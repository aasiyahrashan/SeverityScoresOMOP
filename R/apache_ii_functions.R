#' Calculates emergency admission variable for APACHE II.
#' Assumes elective if there is no data.
#'
#' @param data
#' Dataframe containing physiology variables and units of measure.
#' Should be the output of the get_score_variables function with the
#' severity score parameter set to "APACHE II"
#'
#' @import data.table
#' @returns
#' A data frame with the physiology values converted
#' to the default units of measure specified.
emergency_admission <- function(data) {
  #### Assuming everyone is a planned admission if no information.
  if (!"emergency_admission" %in% names(data)) {
    data[, emergency_admission := 0]
  }

  ### Using emergency admisison variable if it exists
  if ("count_emergency_admission" %in% names(data)) {
    data[count_emergency_admission > 0, emergency_admission := 1]
  }

  data
}

#' Calculates renal failure variable for APACHE II.
#' Assumes no renal failure if no data.
#'
#' @param data
#' Dataframe containing physiology variables and units of measure. Should be the
#' output of the get_score_variables function with the 'severity score parameter
#' set to APACHE II"
#'
#' @import data.table
#' @return
#' A data frame with the physiology values converted
#' to the default units of measure specified.
renal_failure <- function(data) {
  #### Assuming no renal failure if there's no record
  if (!"renal_failure" %in% names(data)) {
    data[, renal_failure := 0]
  }

  ### Using variable if it exists
  if ("count_renal_failure" %in% names(data)) {
    data[count_renal_failure > 0, renal_failure := 1]
  }

  data
}

#' Calculates comorbidity variable for APACHE II.
#' Assumes no comorbidities if no data.
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
comorbidities <- function(data) {
  #### Assuming no comorbidities if there's no data
  if (!"comorbidity" %in% names(data)) {
    data[, comorbidity := 0]
  }

  ### Using variable if it exists
  if ("count_comorbidity" %in% names(data)) {
    data[count_comorbidity > 0, comorbidity := 1]
  }

  data
}
