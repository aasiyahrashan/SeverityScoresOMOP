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

#' Convert units of measure into a format for the SOFA score calculation
#'
#' Assumes units of measure are encoded in OMOP using the UCUM source vocabulary
#' Throws a warning if the unit of measure is not recognised. Assumes the
#' default unit of measure if not available.
#'
#' @param data
#' Dataframe containing physiology variables and units of measure. Should be the
#' output of the get_score_variables function with the severity score parameter
#' set to "SOFA"
#'
#' @import data.table
#' @returns
#' A data frame with physiology values converted to
#' the default units of measure specified.
#' @export
fix_sofa_units <- function(data) {
  data <- as.data.table(data)

  # FiO2. Not bothering with unit of measure. Going with whether it's a ratio or percentage.
  # Making it a ratio.
  data[max_fio2 > 1, max_fio2 := max_fio2 / 100]
  data[min_fio2 > 1, min_fio2 := min_fio2 / 100]

  data[, unit_fio2 := "ratio"]

  # Default unit for pao2 is millimeter mercury column
  data[unit_pao2 == "kilopascal", max_pao2 := max_pao2 * 7.50062]
  data[unit_pao2 == "kilopascal", min_pao2 := min_pao2 * 7.50062]

  if (!all(unique(data$unit_pao2) %in% c("kilopascal", "millimeter mercury column", NA))) {
    warning("pao2 contains an unknown unit of measure. Assuming values are millimeter mercury column")
  }
  data[, unit_pao2 := "millimeter mercury column"]

  # Default unit for creatinine is milligram per deciliter
  data[unit_creatinine == "micromole per liter", max_creatinine := max_creatinine * 0.0113]
  data[unit_creatinine == "micromole per liter", min_creatinine := min_creatinine * 0.0113]

  data[unit_creatinine == "millimole per liter", max_creatinine := max_creatinine * 11.312]
  data[unit_creatinine == "millimole per liter", min_creatinine := min_creatinine * 11.312]

  data[unit_creatinine == "milligram per liter", max_creatinine := max_creatinine * 0.1]
  data[unit_creatinine == "milligram per liter", min_creatinine := min_creatinine * 0.1]

  if (!all(unique(data$unit_creatinine) %in% c(
    "milligram per deciliter", "micromole per liter",
    "millimole per liter", "milligram per deciliter", NA
  ))) {
    warning("creatinine contains an unknown unit of measure. Assuming values are milligram per deciliter")
  }
  data[, unit_creatinine := "milligram per deciliter"]

  # Default unit for bilirubin is miligram per decliter
  data[unit_bilirubin == "micromole per liter", max_bilirubin := max_bilirubin * 0.0585]
  data[unit_bilirubin == "micromole per liter", min_bilirubin := min_bilirubin * 0.0585]

  if (!all(unique(data$unit_bilirubin) %in% c(
    "micromole per liter", "milligram per deciliter", NA))) {
    warning("bilirubin contains an unknown unit of measure. Assuming values are milligram per deciliter")
  }
  data[, unit_bilirubin := "milligram per deciliter"]

  # Default unit for platelets is is billion per liter.
  # Which is the same as thousand per cubic millimeter. And thousand per microliter.

  # /mm3, /uL
  data[unit_platelet  %in% c("per microliter", "cells per microliter",
                            "per cubic millimeter", "cells per cubic millimeter"),
       max_platelet := max_platelet / 1000]
  data[unit_platelet %in% c("per microliter", "cells per microliter",
                           "per cubic millimeter", "cells per cubic millimeter"),
       min_platelet := min_platelet / 1000]

  # per liter
  data[unit_platelet == "per liter", max_platelet := max_platelet * 0.000000001]
  data[unit_platelet == "per liter", min_platelet := min_platelet * 0.000000001]

  # lakhs/mm3
  data[unit_platelet == "lakhs/mm3", max_platelet := max_platelet * 100]
  data[unit_platelet == "lakhs/mm3", min_platelet := min_platelet * 100]

  if (!all(unique(data$unit_platelet) %in% c(
    "billion per liter", "thousand per cubic millimeter",
    "billion cells per liter", "thousand per microliter",
    "per cubic millimeter", "cells per cubic millimeter",
    "per liter", "lakhs/mm3", "per microliter", "cells per microliter",
    NA
  ))) {
    warning("White cell count contains an unknown unit of measure. Assuming values are in billion per liter")
  }
  data[, unit_platelet := "billion per liter"]

  # Default unit for blood pressure is mmhg.

  data
}

#' Removes implausible values from variables used to calculate the SOFA score.
#' Assumes the units of measure have been fixed using the fix_sofa_units function.
#'
#' @param data Dataframe containing physiology variables and units of measure.
#' Should be the output of the get_score_variables function with the 'severity score parameter set to "SOFA"
#'
#' @return A data frame with implausible physiology values deleted.
#' @import data.table
#' @export
fix_implausible_values_sofa <- function(data) {
  # platelets
  data[(max_platelet < 0 | max_wcc > 9999) & unit_platelet == "billion per liter", max_platelet := NA]

  data[(min_platelet < 0 | min_wcc > 9999) & unit_platelet == "billion per liter", min_platelet := NA]

  if (!all(unique(data$unit_platelet) %in% c("billion per liter", NA))) {
    warning("Platelet count values are not in billion per liter. Please fix before attempting to delete implausible values.")
  }

  # bilirubin
  data[(max_bilirubin < 0 | max_bilirubin > 100) & unit_bilirubin == "milligram per deciliter", max_bilirubin := NA]

  data[(min_bilirubin < 0 | min_bilirubin > 100) & unit_bilirubin == "milligram per deciliter", min_bilirubin := NA]

  if (!all(unique(data$unit_bilirubin) %in% c("milligram per deciliter", NA))) {
    warning("Bilirubin values are not in milligram per deciliter. Please fix before attempting to delete implausible values.")
  }

  # FiO2.
  data[(max_fio2 < 0.21 | max_fio2 > 1) & unit_fio2 == "ratio", max_fio2 := NA]

  data[(min_fio2 < 0.21 | min_fio2 > 1) & unit_fio2 == "ratio", min_fio2 := NA]

  if (!all(unique(data$unit_fio2) %in% c("ratio", NA))) {
    warning("FiO2 values are not in ratio. Please fix before attempting to delete implausible values.")
  }

  # PaO2
  data[(max_pao2 < 20 | max_pao2 > 400) &
         unit_pao2 == "millimeter mercury column", max_pao2 := NA]

  data[(min_pao2 < 20 | min_pao2 > 400) &
         unit_pao2 == "millimeter mercury column", min_pao2 := NA]

  if (!all(unique(data$unit_pao2) %in% c("millimeter mercury column", NA))) {
    warning("pao2 is not in millimeter mercury column. Please fix before attempting to delete implausible values.")
  }

  # Creatinine
  data[(max_creatinine < 0.1 | max_creatinine > 39) &
         unit_creatinine == "milligram per deciliter", max_creatinine := NA]

  data[(min_creatinine < 0.1 | min_creatinine > 39) &
         unit_creatinine == "milligram per deciliter", min_creatinine := NA]

  if (!all(unique(data$unit_creatinine) %in% c("milligram per deciliter", NA))) {
    warning("Creatinine is not in milligram per deciliter. Please fix before attempting to delete implausible values.")
  }

  # Respiratory rate
  data[max_rr < 2 | max_rr > 79, max_rr := NA]
  data[min_rr < 2 | min_rr > 79, min_rr := NA]

  # Blood pressure. Doing it separately for MAP and for SBP and DBP.
  if (!"min_map" %in% names(data)) {
    # Assuming bp variables will be called sbp and dbp if not map
    if ("min_dbp" %in% names(data) &
        "min_sbp" %in% names(data)) {
      data[max_sbp < 20 | max_sbp > 299, max_sbp := NA]
      data[min_sbp < 20 | min_sbp > 299, min_sbp := NA]

      data[max_dbp < 0 | max_dbp > 200, max_dbp := NA]
      data[min_dbp < 0 | min_dbp > 200, min_dbp := NA]
    }
  }

  if ("min_map" %in% names(data)) {
    data[max_map < 6 | max_map > 233, max_map := NA]
    data[min_map < 6 | min_map > 233, min_map := NA]
  }

  data
}

