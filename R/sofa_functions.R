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
  data[(max_platelet < 0 | max_platelet > 9999) & unit_platelet == "billion per liter", max_platelet := NA]

  data[(min_platelet < 0 | min_platelet > 9999) & unit_platelet == "billion per liter", min_platelet := NA]

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

#' Calculates the SOFA score.
#' Assumes the units of measure have been fixed using the fix_sofa_units function.
#' Missing data is handled using normal imputation by default. If 'none', there is no imputation.
#' @param data Dataframe containing physiology variables and units of measure.
#' @param imputation
#' Should be the output of the get_score_variables function with the 'severity score parameter set to SOFA
#'
#' @return A data frame with a variable containing the apache II score calculated.
#' @import data.table
#' @export
calculate_sofa_score <- function(data, imputation = "normal") {
  data <- as.data.table(data)

  # Define the fields requested for full computation
  # Left out the blood pressure, gcs and mechanical ventilation variables
  # since they may be calculated within this function.
  sofa <- c(
    "min_platelet",
    "max_fio2", "min_pao2",
    "max_creatinine", "max_bilirubin"
  )

  # Display a warning if fields are missing
  if (all(!sofa %in% names(data))) {
    warning("Some of the variables required for the SOFA calculation are missing.
            Please make sure get_score_variables function has been run, and the concepts
            file includes all variables.")
  }

  ##### THE ORDER OF CONDITIONS IS IMPORTANT FOR ALL THE BLOCKS BELOW.

  #### Creating new variables which will contain the APACHE sub scores.
  subscore_variables <- c(
    "pf_ratio_sofa", "min_platelet_sofa", "max_bilirubin_sofa",
    "min_map_sofa", "max_creat_sofa", "min_gcs_sofa")

  if (imputation == "normal") {
    ### Subscores are assigned a score of 0 if there is normal imputation, and no data for variables.
    data[, (subscore_variables) := as.integer(0)]
    total_variable <- "sofa_score"
  } else if (imputation == "none") {
    ### The score variable is empty if there is no physiology value available.
    data[, (subscore_variables) := as.integer(NA)]
    total_variable <- "sofa_score_no_imputation"
  } else {
    warning("The imputation type should either be 'normal' or 'none'")
  }

  # Respiratory. Need to know PF ratio and if patient was on MV
  data <- mechanical_ventilation(data)
  # Choosing worst values
  data[, pf_ratio := min_pao2/max_fio2]

  # PF ratio
  data[pf_ratio < 100 & mechanical_ventilation == 1, pf_ratio_sofa := 4]
  data[pf_ratio >= 100 & pf_ratio < 200 & mechanical_ventilation == 1, pf_ratio_sofa := 3]
  data[pf_ratio >= 200 & pf_ratio < 300, pf_ratio_sofa := 2]
  data[pf_ratio >= 300 & pf_ratio < 400, pf_ratio_sofa := 1]
  data[pf_ratio >= 400, pf_ratio_sofa := 0]


  # Platelets Using min becasue it always gets a higher score
  data[min_platelet < 20, min_platelet_sofa := 4]
  data[min_platelet >= 20 & min_platelet < 50, min_platelet_sofa := 3]
  data[min_platelet >= 50 & min_platelet < 100, min_platelet_sofa := 2]
  data[min_platelet >= 100 & min_platelet < 150, min_platelet_sofa := 1]
  data[min_platelet >= 150, min_platelet_sofa := 0]

  # Bilirubin. Using max becasue it always gets a higher score
  data[max_bilirubin >= 12, max_bilirubin_sofa := 4]
  data[max_bilirubin >= 6 & max_bilirubin < 12, max_bilirubin_sofa := 3]
  data[max_bilirubin >= 2 & max_bilirubin < 6, max_bilirubin_sofa := 2]
  data[max_bilirubin >= 1.2 & max_bilirubin < 2, max_bilirubin_sofa := 1]
  data[max_bilirubin <1.2, max_bilirubin_sofa := 0]

  # Mean arterial pressure. Calculating MAP first.
  data <- mean_arterial_pressure(data)

  # NOTE - Must calculate score based on vasopressors as well.
  data[(min_map <70), min_map_sofa := 1]
  data[(min_map >= 70), min_map_sofa := 0]

  # Creatinine. Using max becasue it always gets a higher score
  data[max_creatinine >= 5, max_creat_sofa := 4]
  data[max_creatinine >= 3.5 & max_creatinine < 5, max_creat_sofa := 3]
  data[max_creatinine >= 2 & max_creatinine < 3.5, max_creat_sofa := 2]
  data[max_creatinine >= 1.2 & max_creatinine < 2, max_creat_sofa := 1]
  data[max_creatinine <1.2, max_creat_sofa := 0]

  # GCS. Using min because it always get a higher score
  data[min_gcs < 6, min_gcs_sofa := 4]
  data[min_gcs >= 6 & min_gcs < 10, min_gcs_sofa := 3]
  data[min_gcs >= 10 & min_gcs < 13, min_gcs_sofa := 2]
  data[min_gcs >= 13 & min_gcs < 15, min_gcs_sofa := 1]
  data[min_gcs == 15, min_gcs_sofa := 0]

  # Total SOFA
  data[, (total_variable) := pf_ratio_sofa + min_platelet_sofa +
         max_bilirubin_sofa + min_map_sofa
       + max_creat_sofa + min_gcs_sofa]

  ### Deleting the intermediate variables so the dataset is not too long.
  data[, (subscore_variables) := NULL]
  data
}
