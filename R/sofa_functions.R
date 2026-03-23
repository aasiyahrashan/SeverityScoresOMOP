#' Calculates mechanical ventilation variable for SOFA
#' This should include both invasive and non-invasive ventilation.
#'
#' @param data
#' Dataframe containing physiology variables and units of measure.
#' Should be the output of the get_score_variables function with the
#' severity score parameter set to "SOFA"
#'
#' @import data.table
#' @returns
#' A data frame with the physiology values converted
#' to the default units of measure specified.
mechanical_ventilation <- function(data) {
  # Assumes no mechanical ventilation if no information.
  if (!"mechanical_ventilation" %in% names(data)) {
    data[, mechanical_ventilation := 0]
  }

  # Using mechanical ventilation variable if it exists
  if ("count_mechanical_ventilation" %in% names(data)) {
    data[count_mechanical_ventilation > 0, mechanical_ventilation := 1]
  }

  data
}
