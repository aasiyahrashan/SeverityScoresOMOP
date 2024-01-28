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
