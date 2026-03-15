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
