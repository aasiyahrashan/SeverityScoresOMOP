#' Check that required columns exist in a dataframe.
#'
#' Stops with a clear error listing missing columns and suggesting a fix.
#' Used internally by scoring and unit-fixing functions.
#'
#' @param data A dataframe to check.
#' @param required_cols Character vector of column names that must be present.
#' @param fn_name Name of the calling function, used in the error message.
#'
#' @return NULL, invisibly. Called for its side effect (stopping on error).
check_required_columns <- function(data, required_cols, fn_name) {
  missing <- setdiff(required_cols, names(data))
  if (length(missing) > 0) {
    stop(fn_name, ": missing required column(s): ",
         paste(missing, collapse = ", "),
         ". Check that get_score_variables has been run and the concepts ",
         "file includes all required variables.",
         call. = FALSE)
  }
  invisible(NULL)
}


#' Calculates mean arterial pressure if not already calculated.
#'
#' @param data
#' Dataframe containing physiology variables and units of measure. Should be the
#' output of the get_score_variables function with the severity score parameter
#' set to "APACHE II"
#'
#' @import data.table
#' @return
#' A data frame with the physiology values converted
#' to the default units of measure specified.
mean_arterial_pressure <- function(data) {
  ## If min exists, the max has to as well.
  if (!"min_map" %in% names(data)) {
    ### Assuming bp variables will be called sbp and dbp if not map
    if ("min_dbp" %in% names(data) &
        "min_sbp" %in% names(data)) {
      ### Calculating MAP
      data[, min_map := min_dbp + 1 / 3 * (min_sbp - min_dbp)]
      data[, max_map := max_dbp + 1 / 3 * (max_sbp - max_dbp)]
    }
  }
  data
}

#' Calculates total gcs if not already calculated.
#'
#' @param data
#' Dataframe containing physiology variables and units of measure. Should be the
#' output of the get_score_variables function with the severity score parameter
#' set to "APACHE II"
#'
#' @import data.table
#' @return
#' A data frame with the physiology values converted
#' to the default units of measure specified.
total_gcs <- function(data) {

  ## If min exists, the max has to as well.
  if (!"min_gcs" %in% names(data)) {
    #### Adding combined gcs scores stored as numbers to data.
    data[, min_gcs := min_gcs_eye + min_gcs_verbal + min_gcs_motor]
    data[, max_gcs := max_gcs_eye + max_gcs_verbal + max_gcs_motor]
  }
  data
}


#' Convert count columns to binary (0/1) indicator columns.
#'
#' Finds all columns starting with \code{count_}, creates a new column
#' with the \code{count_} prefix stripped (e.g. \code{count_emergency_admission}
#' becomes \code{emergency_admission}), and sets it to 1 if the count > 0, else 0.
#' NA counts remain NA.
#'
#' If a column with the target name already exists, it is overwritten with
#' a warning.
#'
#' @param data A data.frame or data.table.
#' @param drop_counts If TRUE (default), removes the original count_ columns
#'   after creating the binary versions.
#'
#' @return The data.table with binary indicator columns added.
#' @import data.table
#' @export
binarise_counts <- function(data, drop_counts = TRUE) {
  data <- as.data.table(data)

  count_cols <- grep("^count_", names(data), value = TRUE)
  if (length(count_cols) == 0) {
    message("No count_ columns found.")
    return(data)
  }

  binary_names <- sub("^count_", "", count_cols)

  # Warn about overwrites
  existing <- intersect(binary_names, names(data))
  if (length(existing) > 0) {
    warning("Overwriting existing column(s): ",
            paste(existing, collapse = ", "))
  }

  for (i in seq_along(count_cols)) {
    src <- count_cols[i]
    dst <- binary_names[i]
    data[, (dst) := fifelse(is.na(get(src)), NA_integer_,
                            fifelse(get(src) > 0, 1L, 0L))]
  }

  if (drop_counts) {
    data[, (count_cols) := NULL]
  }

  data
}

#' Add length of stay columns.
#'
#' Calculates ICU and hospital length of stay as both calendar days
#' (date difference + 1, so same-day admission/discharge = 1 day) and
#' fractional hours (datetime difference, no +1).
#'
#' Datetime columns are coerced to POSIXct if not already.
#'
#' @section Timezone note:
#' \code{as.POSIXct()} uses the system timezone if the input is character
#' without timezone info. If your database returns datetimes as character
#' with timezone offsets (e.g. \code{"+01:00"}), the conversion may shift
#' dates at midnight boundaries, changing \code{los_days} by +/-1.
#' If this is a concern, ensure datetimes are POSIXct with the correct
#' timezone before calling this function.
#'
#' @param data A data.frame or data.table with admission/discharge columns.
#'
#' @return The data.table with columns added:
#'   \code{icu_los_days}, \code{icu_los_hours},
#'   \code{hospital_los_days}, \code{hospital_los_hours}.
#'   Returns NA for a row if the required datetime columns are missing or NA.
#' @import data.table
#' @export
add_length_of_stay <- function(data) {
  data <- as.data.table(data)

  if (all(c("icu_admission_datetime", "icu_discharge_datetime") %in% names(data))) {
    adm <- as.POSIXct(data$icu_admission_datetime)
    dis <- as.POSIXct(data$icu_discharge_datetime)
    data[, icu_los_days := as.integer(as.Date(dis) - as.Date(adm)) + 1L]
    data[, icu_los_hours := as.numeric(difftime(dis, adm, units = "hours"))]
  } else {
    warning("Cannot calculate ICU LOS: missing icu_admission_datetime or icu_discharge_datetime.")
  }

  if (all(c("hospital_admission_datetime", "hospital_discharge_datetime") %in% names(data))) {
    adm <- as.POSIXct(data$hospital_admission_datetime)
    dis <- as.POSIXct(data$hospital_discharge_datetime)
    data[, hospital_los_days := as.integer(as.Date(dis) - as.Date(adm)) + 1L]
    data[, hospital_los_hours := as.numeric(difftime(dis, adm, units = "hours"))]
  } else {
    warning("Cannot calculate hospital LOS: missing hospital_admission_datetime or hospital_discharge_datetime.")
  }

  data
}


#' Add mortality flag columns.
#'
#' Creates \code{icu_mortality} and \code{hospital_mortality} as 0/1 integer
#' columns based on whether \code{death_datetime} falls on or before the
#' respective discharge datetime.
#'
#' @section Timezone note:
#' Same caveat as \code{\link{add_length_of_stay}} — if datetimes are
#' character strings with timezone offsets, \code{as.POSIXct()} may shift
#' them during conversion. Ensure consistent timezone handling upstream.
#'
#' Logic:
#' \itemize{
#'   \item If \code{death_datetime} is NA → 0 (assumed alive).
#'   \item If \code{death_datetime <= icu_discharge_datetime} → \code{icu_mortality = 1}.
#'   \item If \code{death_datetime <= hospital_discharge_datetime} → \code{hospital_mortality = 1}.
#'   \item If \code{death_datetime} is present but discharge datetime is NA →
#'     the flag will be NA. The user must decide how to handle these cases
#'     (e.g. impute discharge time, treat as died, or exclude).
#' }
#'
#' @param data A data.frame or data.table with \code{death_datetime} and
#'   discharge datetime columns.
#'
#' @return The data.table with \code{icu_mortality} and \code{hospital_mortality}
#'   columns added.
#' @import data.table
#' @export
add_mortality_flags <- function(data) {
  data <- as.data.table(data)

  if (!"death_datetime" %in% names(data)) {
    warning("Cannot calculate mortality: missing death_datetime column.")
    return(data)
  }

  death <- as.POSIXct(data$death_datetime)

  if ("icu_discharge_datetime" %in% names(data)) {
    icu_dis <- as.POSIXct(data$icu_discharge_datetime)
    data[, icu_mortality := fifelse(
      is.na(death), 0L,
      fifelse(death <= icu_dis, 1L, 0L)
    )]
  } else {
    warning("Cannot calculate ICU mortality: missing icu_discharge_datetime.")
  }

  if ("hospital_discharge_datetime" %in% names(data)) {
    hosp_dis <- as.POSIXct(data$hospital_discharge_datetime)
    data[, hospital_mortality := fifelse(
      is.na(death), 0L,
      fifelse(death <= hosp_dis, 1L, 0L)
    )]
  } else {
    warning("Cannot calculate hospital mortality: missing hospital_discharge_datetime.")
  }

  data
}
