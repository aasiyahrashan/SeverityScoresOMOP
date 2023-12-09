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
  data[, emergency_admission := 0]

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
  data[, renal_failure := 0]

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
  data[, comorbidity := 0]

  ### Using variable if it exists
  if ("count_comorbidity" %in% names(data)) {
    data[count_comorbidity > 0, comorbidity := 1]
  }

  data
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

#' Convert units of measure into a format for the APACHE II score calculation
#'
#' Assumes units of measure are encoded in OMOP using the UCUM source vocabulary
#' Throws a warning if the unit of measure is not recognised. Assumes the
#' default unit of measure if not available.
#'
#' @param data
#' Dataframe containing physiology variables and units of measure. Should be the
#' output of the get_score_variables function with the severity score parameter
#' set to "APACHE II"
#'
#' @import data.table
#' @returns
#' A data frame with physiology values converted to
#' the default units of measure specified.
#' @export
fix_apache_ii_units <- function(data) {
  data <- as.data.table(data)

  #### Default unit for temperature is celsius.
  #### It's more accurate to divide based on values instead of what the unit says.
  data[max_temp > 50, max_temp := (max_temp - 32) * 5 / 9]
  data[min_temp > 50, min_temp := (min_temp - 32) * 5 / 9]

  if (!all(unique(data$unit_temp) %in% c("degree Celsius", "degree Fahrenheit", NA))) {
    warning("Temperature contains an unknown unit of measure. Assuming values are in Celsius")
  }
  data[, unit_temp := "degree Celsius"]

  #### Default unit for white cell count is billion per liter.
  #### Which is the same as thousand per cubic millimeter. And thousand per microliter.
  # /mm3
  data[unit_wcc == "per cubic millimeter", max_wcc := max_wcc / 1000]
  data[unit_wcc == "per cubic millimeter", min_wcc := min_wcc / 1000]

  # cells per cubic millimeter
  data[unit_wcc == "cells per cubic millimeter", max_wcc := max_wcc / 1000]
  data[unit_wcc == "cells per cubic millimeter", min_wcc := min_wcc / 1000]

  # per liter
  data[unit_wcc == "per liter", max_wcc := max_wcc * 0.000000001]
  data[unit_wcc == "per liter", min_wcc := min_wcc * 0.000000001]

  # lakhs/mm3
  data[unit_wcc == "lakhs/mm3", max_wcc := max_wcc * 100]
  data[unit_wcc == "lakhs/mm3", min_wcc := min_wcc * 100]

  if (!all(unique(data$unit_wcc) %in% c(
    "billion per liter", "thousand per cubic millimeter",
    "billion cells per liter", "thousand per microliter",
    "per cubic millimeter", "cells per cubic millimeter",
    "per liter", "lakhs/mm3",
    NA
  ))) {
    warning("White cell count contains an unknown unit of measure. Assuming values are in billion per liter")
  }
  data[, unit_wcc := "billion per liter"]

  #### FiO2. Not bothering with unit of measure. Going with whether it's a ratio or percentage.
  #### Making it a ratio.
  data[max_fio2 > 1, max_fio2 := max_fio2 / 100]
  data[min_fio2 > 1, min_fio2 := min_fio2 / 100]

  data[, unit_fio2 := "ratio"]

  #### Default unit for pao2 is millimeter mercury column
  data[unit_pao2 == "kilopascal", max_pao2 := max_pao2 * 7.50062]
  data[unit_pao2 == "kilopascal", min_pao2 := min_pao2 * 7.50062]

  if (!all(unique(data$unit_pao2) %in% c("kilopascal", "millimeter mercury column", NA))) {
    warning("pao2 contains an unknown unit of measure. Assuming values are millimeter mercury column")
  }
  data[, unit_pao2 := "millimeter mercury column"]

  #### Default unit for paco2 is millimeter mercury column
  data[unit_paco2 == "kilopascal", max_paco2 := max_paco2 * 7.50062]
  data[unit_paco2 == "kilopascal", min_paco2 := min_paco2 * 7.50062]

  if (!all(unique(data$unit_paco2) %in% c("kilopascal", "millimeter mercury column", NA))) {
    warning("paco2 contains an unknown unit of measure. Assuming values are millimeter mercury column")
  }
  data[, unit_paco2 := "millimeter mercury column"]

  #### Default unit for hematocrit is percent
  data[unit_hematocrit == "liter per liter", max_hematocrit := max_hematocrit * 100]
  data[unit_hematocrit == "liter per liter", min_hematocrit := min_hematocrit * 100]

  data[unit_hematocrit == "ratio", max_hematocrit := max_hematocrit * 100]
  data[unit_hematocrit == "ratio", min_hematocrit := min_hematocrit * 100]

  if (!all(unique(data$unit_hematocrit) %in% c("liter per liter", "percent", "ratio", NA))) {
    warning("hematocrit contains an unknown unit of measure. Assuming values are liter per liter")
  }
  data[, unit_hematocrit := "percent"]

  #### Default unit for sodium is millimole per liter.
  #### This is also the same as milliequivalent per liter
  data[unit_sodium == "millimole per deciliter", max_sodium := max_sodium * 10]
  data[unit_sodium == "millimole per deciliter", min_sodium := min_sodium * 10]

  if (!all(unique(data$unit_sodium) %in% c(
    "millimole per liter", "millimole per deciliter",
    "milliequivalent per liter", NA
  ))) {
    warning("sodium contains an unknown unit of measure. Assuming values are millimole per liter")
  }
  data[, unit_sodium := "millimole per liter"]

  #### Default unit for potassium is millimole per liter.
  #### Which is the same as milliequivalent per liter
  data[unit_potassium == "millimole per deciliter", max_potassium := max_potassium * 10]
  data[unit_potassium == "millimole per deciliter", min_potassium := min_potassium * 10]

  if (!all(unique(data$unit_potassium) %in% c(
    "millimole per liter", "millimole per deciliter",
    "milliequivalent per liter", NA
  ))) {
    warning("potassium contains an unknown unit of measure. Assuming values are millimole per liter")
  }
  data[, unit_potassium := "millimole per liter"]

  #### Default unit for creatinine is milligram per deciliter
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

  #### Default unit for bicarbonate is millimole per liter.
  #### Which is the same as milliequivalent per liter.
  if (!all(unique(data$unit_bicarbonate) %in% c(
    "millimole per liter",
    "milliequivalent per liter", NA
  ))) {
    warning("bicarbonate contains an unknown unit of measure. Assuming values are in millimole per liter")
  }
  data[, unit_bicarbonate := "millimole per liter"]

  #### Default for respiratory rate is breaths per minute.
  #### Default unit for heart rate is beats per minute
  #### Default unit for ph is unitless.
  #### Default unit for blood pressure is mmhg.

  data
}

#' Removes implausible values from variables used to calculate the APACHE II score.
#' Assumes the units of measure have been fixed using the fix_apache_ii_units function.
#'
#' @param data Dataframe containing physiology variables and units of measure.
#' Should be the output of the get_score_variables function with the 'severity score parameter set to APACHE II"
#'
#' @return A data frame with implausible physiology values deleted.
#' @import data.table
#' @export
fix_implausible_values_apache_ii <- function(data) {
  ### Temperature.
  data[(max_temp < 25 | max_temp > 49.9) & unit_temp == "degree Celsius", max_temp := NA]

  data[(min_temp < 25 | min_temp > 49.9) & unit_temp == "degree Celsius", min_temp := NA]

  if (!all(unique(data$unit_temp) %in% c("degree Celsius", NA))) {
    warning("Temperature values are not in celcius. Please fix before attempting to delete implausible values.")
  }

  #### white cell count
  data[(max_wcc < 0 | max_wcc > 9999) & unit_wcc == "billion per liter", max_wcc := NA]

  data[(min_wcc < 0 | min_wcc > 9999) & unit_wcc == "billion per liter", min_wcc := NA]

  if (!all(unique(data$unit_wcc) %in% c("billion per liter", NA))) {
    warning("White cell count values are not in billion per liter. Please fix before attempting to delete implausible values.")
  }

  ### FiO2.
  data[(max_fio2 < 0.21 | max_fio2 > 1) & unit_fio2 == "ratio", max_fio2 := NA]

  data[(min_fio2 < 0.21 | min_fio2 > 1) & unit_fio2 == "ratio", min_fio2 := NA]

  if (!all(unique(data$unit_fio2) %in% c("ratio", NA))) {
    warning("FiO2 values are not in ratio. Please fix before attempting to delete implausible values.")
  }

  #### PaO2
  data[(max_pao2 < 20 | max_pao2 > 400) &
    unit_pao2 == "millimeter mercury column", max_pao2 := NA]

  data[(min_pao2 < 20 | min_pao2 > 400) &
    unit_pao2 == "millimeter mercury column", min_pao2 := NA]

  if (!all(unique(data$unit_pao2) %in% c("millimeter mercury column", NA))) {
    warning("pao2 is not in millimeter mercury column. Please fix before attempting to delete implausible values.")
  }

  #### PaCO2.
  data[(max_paco2 < 0 | max_paco2 > 376) &
    unit_paco2 == "millimeter mercury column", max_paco2 := NA]

  data[(min_paco2 < 0 | min_paco2 > 376) &
    unit_paco2 == "millimeter mercury column", min_paco2 := NA]

  if (!all(unique(data$unit_paco2) %in% c("millimeter mercury column", NA))) {
    warning("pcao2 is not in millimeter mercury column. Please fix before attempting to delete implausible values.")
  }

  #### Hematocrit
  data[(max_hematocrit < 0 | max_hematocrit > 100) &
    unit_hematocrit == "percent", max_hematocrit := NA]

  data[(min_hematocrit < 0 | min_hematocrit > 100) &
    unit_hematocrit == "percent", min_hematocrit := NA]

  if (!all(unique(data$unit_hematocrit) %in% c("percent", NA))) {
    warning("Hematocrit is not in percent. Please fix before attempting to delete implausible values.")
  }

  #### Sodium
  data[(max_sodium < 40 | max_sodium > 260) &
    unit_sodium == "millimole per liter", max_sodium := NA]

  data[(min_sodium < 40 | min_sodium > 260) &
    unit_sodium == "millimole per liter", min_sodium := NA]

  if (!all(unique(data$unit_sodium) %in% c("millimole per liter", NA))) {
    warning("Sodium is not in millimole per liter. Please fix before attempting to delete implausible values.")
  }

  #### Potassium
  data[(max_potassium < 1 | max_potassium > 9.9) &
    unit_potassium == "millimole per liter", max_potassium := NA]

  data[(min_potassium < 1 | min_potassium > 9.9) &
    unit_potassium == "millimole per liter", min_potassium := NA]

  if (!all(unique(data$unit_potassium) %in% c("millimole per liter", NA))) {
    warning("Potassium is not in millimole per liter. Please fix before attempting to delete implausible values.")
  }

  #### Creatinine
  data[(max_creatinine < 0.1 | max_creatinine > 39) &
    unit_creatinine == "milligram per deciliter", max_creatinine := NA]

  data[(min_creatinine < 0.1 | min_creatinine > 39) &
    unit_creatinine == "milligram per deciliter", min_creatinine := NA]

  if (!all(unique(data$unit_creatinine) %in% c("milligram per deciliter", NA))) {
    warning("Creatinine is not in milligram per deciliter. Please fix before attempting to delete implausible values.")
  }

  #### Bicarbonate
  data[(max_bicarbonate < 1 | max_bicarbonate > 59) &
    unit_bicarbonate == "millimole per liter", max_bicarbonate := NA]

  data[(min_bicarbonate < 1 | min_bicarbonate > 59) &
    unit_bicarbonate == "millimole per liter", min_bicarbonate := NA]

  if (!all(unique(data$unit_bicarbonate) %in% c("millimole per liter", NA))) {
    warning("Bicarbonate is not in milligram per deciliter. Please fix before attempting to delete implausible values.")
  }

  #### Respiratory rate
  data[max_rr < 2 | max_rr > 79, max_rr := NA]
  data[min_rr < 2 | min_rr > 79, min_rr := NA]

  #### Heart rate
  data[max_hr < 10 | max_hr > 299, max_hr := NA]
  data[min_hr < 10 | min_hr > 299, min_hr := NA]

  #### pH
  data[max_ph < 6.1 | max_ph > 9, max_ph := NA]
  data[min_ph < 6.1 | min_ph > 9, min_ph := NA]

  #### Blood pressure. Doing it separately for MAP and for SBP and DBP.
  if (!"min_map" %in% names(data)) {
    ### Assuming bp variables will be called sbp and dbp if not map
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


#' Calculates the APACHE II score.
#' Assumes the units of measure have been fixed using the fix_apache_ii_units function.
#' Missing data is handled using normal imputation by default. If 'none', there is no imputation.
#' @param data Dataframe containing physiology variables and units of measure.
#' @param imputation
#' Should be the output of the get_score_variables function with the 'severity score parameter set to APACHE II"
#'
#' @return A data frame with a variable containing the apache II score calculated.
#' @import data.table
#' @export
calculate_apache_ii_score <- function(data, imputation = "normal") {
  data <- as.data.table(data)
  # Define the fields requested for full computation
  # Left out the blood pressure, gcs, renal failure, admission type and comorbidity variables
  # since they may be calculated within this function.
  apache <- c(
    "max_temp", "min_temp", "min_wcc", "max_wcc",
    "max_fio2", "min_paco2", "min_pao2", "min_hematocrit", "max_hematocrit",
    "min_hr", "max_hr", "min_rr", "max_rr", "min_ph", "max_ph",
    "min_bicarbonate", "max_bicarbonate", "min_sodium", "max_sodium",
    "min_potassium", "max_potassium", "min_creatinine",
    "max_creatinine", "age"
  )

  # Display a warning if fields are missing
  if (all(!apache %in% names(data))) {
    warning("Some of the variables required for the APACHE II calculation are missing.
            Please make sure get_score_variables function has been run, and the concepts
            file includes all variables.")
  }

  ##### THE ORDER OF CONDITIONS IS IMPORTANT FOR ALL THE BLOCKS BELOW.

  #### Creating new variables which will contain the APACHE sub scores.
  subscore_variables <- c(
    "max_temp_ap_ii", "min_temp_ap_ii", "min_wcc_ap_ii", "max_wcc_ap_ii",
    "min_map_ap_ii", "max_map_ap_ii", "aado2_ap_ii", "min_hematocrit_ap_ii",
    "max_hematocrit_ap_ii", "min_hr_ap_ii", "max_hr_ap_ii", "min_rr_ap_ii",
    "max_rr_ap_ii", "min_ph_ap_ii", "max_ph_ap_ii", "min_bicarbonate_ap_ii",
    "max_bicarbonate_ap_ii", "min_sodium_ap_ii", "max_sodium_ap_ii",
    "min_potassium_ap_ii", "max_potassium_ap_ii", "gcs_ap_ii", "min_creat_ap_ii",
    "max_creat_ap_ii", "age_ap_ii", "chronic_ap_ii"
  )

  if (imputation == "normal") {
    ### Subscores are assigned a score of 0 if there is normal imputation, and no data for variables.
    data[, (subscore_variables) := as.integer(0)]
    total_variable <- "apache_ii_score"
  } else if (imputation == "none") {
    ### The score variable is empty if there is no physiology value available.
    data[, (subscore_variables) := as.integer(NA)]
    total_variable <- "apache_ii_score_no_imputation"
  } else {
    warning("The imputation type should either be 'normal' or 'none'")
  }

  #### Temperature. Need to calculate for both min and max to work out which is worse.
  data[(min_temp < 30 | min_temp >= 41), min_temp_ap_ii := 4]
  data[(min_temp >= 30 & min_temp < 32) | (min_temp >= 39 & min_temp < 41), min_temp_ap_ii := 3]
  data[(min_temp >= 32 & min_temp < 34), min_temp_ap_ii := 2]
  data[(min_temp >= 34 & min_temp < 36) | (min_temp >= 38.5 & min_temp < 39), min_temp_ap_ii := 1]
  data[(min_temp >= 36 & min_temp < 38.5), min_temp_ap_ii := 0]

  data[(max_temp < 30 | max_temp >= 41), max_temp_ap_ii := 4]
  data[(max_temp >= 30 & max_temp < 32) | (max_temp >= 39 & max_temp < 41), max_temp_ap_ii := 3]
  data[(max_temp >= 32 & max_temp < 34), max_temp_ap_ii := 2]
  data[(max_temp >= 34 & max_temp < 36) | (max_temp >= 38.5 & max_temp < 39), max_temp_ap_ii := 1]
  data[(max_temp >= 36 & max_temp < 38.5), max_temp_ap_ii := 0]

  ##### White blood cell count
  data[(min_wcc >= 3 & min_wcc < 15), min_wcc_ap_ii := 0]
  data[(min_wcc >= 15 & min_wcc < 20), min_wcc_ap_ii := 1]
  data[(min_wcc >= 20 & min_wcc < 40) | (min_wcc >= 1 & min_wcc < 3), min_wcc_ap_ii := 2]
  data[(min_wcc >= 40 | min_wcc < 1), min_wcc_ap_ii := 4]

  data[(max_wcc >= 3 & max_wcc < 15), max_wcc_ap_ii := 0]
  data[(max_wcc >= 15 & max_wcc < 20), max_wcc_ap_ii := 1]
  data[(max_wcc >= 20 & max_wcc < 40) | (max_wcc >= 1 & max_wcc < 3), max_wcc_ap_ii := 2]
  data[(max_wcc >= 40 | max_wcc < 1), max_wcc_ap_ii := 4]

  ###### Mean arterial pressure. Calculating MAP first.
  data <- mean_arterial_pressure(data)

  data[(min_map >= 160 | min_map < 50), min_map_ap_ii := 4]
  data[(min_map >= 130 & min_map < 160), min_map_ap_ii := 3]
  data[(min_map >= 50 & min_map < 70) | (min_map >= 110 & min_map < 130), min_map_ap_ii := 2]
  data[(min_map >= 70 & min_map < 110), min_map_ap_ii := 0]

  data[(max_map >= 160 | max_map < 50), max_map_ap_ii := 4]
  data[(max_map >= 130 & max_map < 160), max_map_ap_ii := 3]
  data[(max_map >= 50 & max_map < 70) | (max_map >= 110 & max_map < 130), max_map_ap_ii := 2]
  data[(max_map >= 70 & max_map < 110), max_map_ap_ii := 0]

  #### AADO2
  ### Calculating the A-a gradient. The highest value is worst, so picking the ones that match it.
  ### NOTE - should probably get matching fio2, pao2, paco2 instead just getting min and max.
  data[, aado2 := (max_fio2 * 710) - (min_paco2 * 1.25) - min_pao2]

  data[max_fio2 >= 0.5 & aado2 >= 500, aado2_ap_ii := 4]
  data[max_fio2 < 0.5 & min_pao2 < 55, aado2_ap_ii := 4]
  data[max_fio2 >= 0.5 & aado2 >= 350 & aado2 < 500, aado2_ap_ii := 3]
  data[max_fio2 < 0.5 & min_pao2 >= 55 & min_pao2 <= 60, aado2_ap_ii := 3]
  data[max_fio2 >= 0.5 & aado2 >= 200 & aado2 < 350, aado2_ap_ii := 2]
  data[max_fio2 < 0.5 & min_pao2 > 60 & min_pao2 <= 70, aado2_ap_ii := 1]
  data[max_fio2 >= 0.5 & aado2 < 200, aado2_ap_ii := 0]
  data[max_fio2 < 0.5 & min_pao2 > 70, aado2_ap_ii := 0]

  #### Hematocrit
  data[(min_hematocrit >= 30 & min_hematocrit < 46), min_hematocrit_ap_ii := 0]
  data[(min_hematocrit >= 46 & min_hematocrit < 50), min_hematocrit_ap_ii := 1]
  data[(min_hematocrit >= 50 & min_hematocrit < 60) |
    (min_hematocrit >= 20 & min_hematocrit < 30), min_hematocrit_ap_ii := 2]
  data[(min_hematocrit >= 0 & min_hematocrit < 20) |
    min_hematocrit >= 60, min_hematocrit_ap_ii := 4]

  data[(max_hematocrit >= 30 & max_hematocrit < 46), max_hematocrit_ap_ii := 0]
  data[(max_hematocrit >= 46 & max_hematocrit < 50), max_hematocrit_ap_ii := 1]
  data[(max_hematocrit >= 50 & max_hematocrit < 60) |
    (max_hematocrit >= 20 & max_hematocrit < 30), max_hematocrit_ap_ii := 2]
  data[(max_hematocrit >= 0 & max_hematocrit < 20) |
    max_hematocrit >= 60, max_hematocrit_ap_ii := 4]

  #### heart rate
  data[(min_hr >= 70 & min_hr < 110), min_hr_ap_ii := 0]
  data[(min_hr >= 55 & min_hr < 70) | (min_hr >= 110 & min_hr < 140), min_hr_ap_ii := 2]
  data[(min_hr >= 40 & min_hr < 55) | (min_hr >= 140 & min_hr < 180), min_hr_ap_ii := 3]
  data[(min_hr >= 180 | min_hr < 40), min_hr_ap_ii := 4]

  data[(max_hr >= 70 & max_hr < 110), max_hr_ap_ii := 0]
  data[(max_hr >= 55 & max_hr < 70) | (max_hr >= 110 & max_hr < 140), max_hr_ap_ii := 2]
  data[(max_hr >= 40 & max_hr < 55) | (max_hr >= 140 & max_hr < 180), max_hr_ap_ii := 3]
  data[(max_hr >= 180 | max_hr < 40), max_hr_ap_ii := 4]

  #### respiratory rate
  data[(min_rr >= 12 & min_rr < 25), min_rr_ap_ii := 0]
  data[(min_rr >= 25 & min_rr < 35) | (min_rr >= 10 & min_rr < 12), min_rr_ap_ii := 1]
  data[(min_rr >= 6 & min_rr < 10), min_rr_ap_ii := 2]
  data[(min_rr >= 35 & min_rr < 50), min_rr_ap_ii := 3]
  data[(min_rr >= 50) | (min_rr < 6), min_rr_ap_ii := 4]

  data[(max_rr >= 12 & max_rr < 25), max_rr_ap_ii := 0]
  data[(max_rr >= 25 & max_rr < 35) | (max_rr >= 10 & max_rr < 12), max_rr_ap_ii := 1]
  data[(max_rr >= 6 & max_rr < 10), max_rr_ap_ii := 2]
  data[(max_rr >= 35 & max_rr < 50), max_rr_ap_ii := 3]
  data[(max_rr >= 50) | (max_rr < 6), max_rr_ap_ii := 4]

  #### Ph and HCO3
  #### Use Ph if available. If not, use HCO3.
  data[(min_ph >= 7.33 & min_ph < 7.5), min_ph_ap_ii := 0]
  data[(min_ph >= 7.5 & min_ph < 7.6), min_ph_ap_ii := 1]
  data[(min_ph >= 7.25 & min_ph < 7.33), min_ph_ap_ii := 2]
  data[(min_ph >= 7.6 & min_ph < 7.7) | (min_ph >= 7.15 & min_ph < 7.25), min_ph_ap_ii := 3]
  data[(min_ph >= 7.7 | min_ph < 7.15), min_ph_ap_ii := 4]

  data[(max_ph >= 7.33 & max_ph < 7.5), max_ph_ap_ii := 0]
  data[(max_ph >= 7.5 & max_ph < 7.6), max_ph_ap_ii := 1]
  data[(max_ph >= 7.25 & max_ph < 7.33), max_ph_ap_ii := 2]
  data[(max_ph >= 7.6 & max_ph < 7.7) | (max_ph >= 7.15 & max_ph < 7.25), max_ph_ap_ii := 3]
  data[(max_ph >= 7.7 | max_ph < 7.15), max_ph_ap_ii := 4]


  ##### bicarbonate Will only get calculated if there is no ph
  data[is.na(min_ph) & (min_bicarbonate >= 22 & min_bicarbonate < 32), min_bicarbonate_ap_ii := 0]
  data[is.na(min_ph) & (min_bicarbonate >= 32 & min_bicarbonate < 41), min_bicarbonate_ap_ii := 1]
  data[is.na(min_ph) & (min_bicarbonate >= 18 & min_bicarbonate < 22), min_bicarbonate_ap_ii := 2]
  data[is.na(min_ph) &
    (min_bicarbonate >= 15 & min_bicarbonate >= 18) |
    (min_bicarbonate >= 41 & min_bicarbonate >= 52), min_bicarbonate_ap_ii := 3]
  data[is.na(min_ph) & (min_bicarbonate < 15 | min_bicarbonate >= 52), min_bicarbonate_ap_ii := 4]

  data[is.na(max_ph) & (max_bicarbonate >= 22 & max_bicarbonate < 32), max_bicarbonate_ap_ii := 0]
  data[is.na(max_ph) & (max_bicarbonate >= 32 & max_bicarbonate < 41), max_bicarbonate_ap_ii := 1]
  data[is.na(max_ph) & (max_bicarbonate >= 18 & max_bicarbonate < 22), max_bicarbonate_ap_ii := 2]
  data[is.na(max_ph) &
    (max_bicarbonate >= 15 & max_bicarbonate >= 18) |
    (max_bicarbonate >= 41 & max_bicarbonate >= 52), max_bicarbonate_ap_ii := 3]
  data[is.na(max_ph) & (max_bicarbonate < 15 | max_bicarbonate >= 52), max_bicarbonate_ap_ii := 4]

  ##### sodium
  data[(min_sodium >= 130 & min_sodium < 150), min_sodium_ap_ii := 0]
  data[(min_sodium >= 150 & min_sodium < 155), min_sodium_ap_ii := 1]
  data[(min_sodium >= 155 & min_sodium < 160) |
    (min_sodium >= 120 & min_sodium < 130), min_sodium_ap_ii := 2]
  data[(min_sodium >= 160 & min_sodium < 180) |
    (min_sodium >= 111 & min_sodium < 120), min_sodium_ap_ii := 3]
  data[(min_sodium >= 180 | min_sodium < 111), min_sodium_ap_ii := 4]

  data[(max_sodium >= 130 & max_sodium < 150), max_sodium_ap_ii := 0]
  data[(max_sodium >= 150 & max_sodium < 155), max_sodium_ap_ii := 1]
  data[(max_sodium >= 155 & max_sodium < 160) |
    (max_sodium >= 120 & max_sodium < 130), max_sodium_ap_ii := 2]
  data[(max_sodium >= 160 & max_sodium < 180) |
    (max_sodium >= 111 & max_sodium < 120), max_sodium_ap_ii := 3]
  data[(max_sodium >= 180 | max_sodium < 111), max_sodium_ap_ii := 4]

  #### potassium
  data[(min_potassium >= 3.5 & min_potassium < 5.5), min_potassium_ap_ii := 0]
  data[(min_potassium >= 5.5 & min_potassium < 6) |
    (min_potassium >= 3 & min_potassium < 3.5), min_potassium_ap_ii := 1]
  data[(min_potassium >= 2.5 & min_potassium < 3), min_potassium_ap_ii := 2]
  data[(min_potassium >= 6 & min_potassium < 7), min_potassium_ap_ii := 3]
  data[(min_potassium >= 7 | min_potassium < 2.5), min_potassium_ap_ii := 4]

  data[(max_potassium >= 3.5 & max_potassium < 5.5), max_potassium_ap_ii := 0]
  data[(max_potassium >= 5.5 & max_potassium < 6) |
    (max_potassium >= 3 & max_potassium < 3.5), max_potassium_ap_ii := 1]
  data[(max_potassium >= 2.5 & max_potassium < 3), max_potassium_ap_ii := 2]
  data[(max_potassium >= 6 & max_potassium < 7), max_potassium_ap_ii := 3]
  data[(max_potassium >= 7 | max_potassium < 2.5), max_potassium_ap_ii := 4]

  #### GCS
  data <- total_gcs(data)
  data[, gcs_ap_ii := 15 - min_gcs]
  if (imputation == "normal") {
    data[is.na(gcs_ap_ii), gcs_ap_ii := 0]
  }

  #### creatinine
  data[min_creatinine >= 3.5, min_creat_ap_ii := 4]
  data[min_creatinine >= 2 & min_creatinine < 3.5, min_creat_ap_ii := 3]
  data[min_creatinine >= 1.5 & min_creatinine < 2, min_creat_ap_ii := 2]
  data[min_creatinine < 0.6, min_creat_ap_ii := 2]
  data[min_creatinine >= 0.6 & min_creatinine < 1.5, min_creat_ap_ii := 0]

  data[max_creatinine >= 3.5, max_creat_ap_ii := 4]
  data[max_creatinine >= 2 & max_creatinine < 3.5, max_creat_ap_ii := 3]
  data[max_creatinine >= 1.5 & max_creatinine < 2, max_creat_ap_ii := 2]
  data[max_creatinine < 0.6, max_creat_ap_ii := 2]
  data[max_creatinine >= 0.6 & max_creatinine < 1.5, max_creat_ap_ii := 0]

  ### The creat score is doubled if there was renal failure.
  data <- renal_failure(data)
  data[renal_failure == 1, min_creat_ap_ii * 2]
  data[renal_failure == 1, max_creat_ap_ii * 2]

  #### Age
  data[age < 45, age_ap_ii := 0]
  data[age >= 45 & age < 55, age_ap_ii := 2]
  data[age >= 55 & age < 65, age_ap_ii := 3]
  data[age >= 65 & age < 75, age_ap_ii := 5]
  data[age >= 75, age_ap_ii := 6]

  #### Assuming there are no comorbidities
  data <- comorbidities(data)

  ### Getting information about admission type emergency or elective.
  data <- emergency_admission(data)

  #### chronic health score
  data[comorbidity == 0, chronic_ap_ii := 0]
  data[comorbidity > 0 & emergency_admission == 0, chronic_ap_ii := 2]
  data[comorbidity > 0 & emergency_admission == 1, chronic_ap_ii := 5]

  ### Getting the worst score for each component and adding to get total APACHE II score.
  ### If either max or min is missing, just using the other.
  data[, (total_variable) :=
    pmax(min_temp_ap_ii, max_temp_ap_ii, na.rm = TRUE) +
    pmax(min_wcc_ap_ii, max_wcc_ap_ii, na.rm = TRUE) +
    pmax(min_map_ap_ii, max_map_ap_ii, na.rm = TRUE) + aado2_ap_ii +
    pmax(min_hematocrit_ap_ii, max_hematocrit_ap_ii, na.rm = TRUE) +
    pmax(min_hr_ap_ii, max_hr_ap_ii, na.rm = TRUE) +
    pmax(min_rr_ap_ii, max_rr_ap_ii, na.rm = TRUE) +
    #### The calculation uses either pH or bicarbonate, not both.
    #### Above, bicarbonate is only calculated if pH is missing.
    pmax(min_ph_ap_ii, max_ph_ap_ii,
      min_bicarbonate_ap_ii, max_bicarbonate_ap_ii,
      na.rm = TRUE
    ) +
    pmax(min_sodium_ap_ii, max_sodium_ap_ii, na.rm = TRUE) +
    pmax(min_potassium_ap_ii, max_potassium_ap_ii, na.rm = TRUE) + gcs_ap_ii +
    pmax(min_creat_ap_ii, max_creat_ap_ii, na.rm = TRUE) + age_ap_ii + chronic_ap_ii]

  ### Deleting the intermediate variables so the dataset is not too long.
  delete_cols <- c(
    "min_temp_ap_ii", "max_temp_ap_ii", "min_wcc_ap_ii", "max_wcc_ap_ii",
    "min_map_ap_ii", "max_map_ap_ii", "aado2_ap_ii", "min_hematocrit_ap_ii",
    "max_hematocrit_ap_ii", "min_hr_ap_ii", "max_hr_ap_ii", "min_rr_ap_ii", "max_rr_ap_ii",
    "min_ph_ap_ii", "max_ph_ap_ii", "min_bicarbonate_ap_ii", "max_bicarbonate_ap_ii",
    "min_sodium_ap_ii", "max_sodium_ap_ii", "min_potassium_ap_ii", "max_potassium_ap_ii",
    "gcs_ap_ii", "min_creat_ap_ii", "max_creat_ap_ii", "age_ap_ii", "chronic_ap_ii"
  )

  data[, (delete_cols) := NULL]
  data
}
