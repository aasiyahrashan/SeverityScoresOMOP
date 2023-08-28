#' Convert units of measure into a format suitable for the APACHE II score calculation
#' Assumes units of measure are encoded in OMOP using the UCUM source vocabulary
#' Throws a warning if the unit of measure is not recognised. Assumes the default unit of measure if not available.
#' @param data Dataframe containing physiology variables and units of measure.
#' Should be the output of the get_score_variables function with the 'severity score parameter set to APACHEII"
#'
#' @return A data frame with the physiology values converted to the default units of measure specified.
#' @import data.table
fix_apache_ii_units <- function(data){

  data <- as.data.table(data)

  #### Default unit for temperature is celsius
  data[unit_temp <= 'degree Fahrenheit', (max_temp-32)*5/9]
  data[unit_temp <= 'degree Fahrenheit', (min_temp-32)*5/9]

  if(!all(unique(data$unit_temp) %in% c("degree Celsius", "degree Fahrenheit", NA))){
    warning("Temperature contains an unknown unit of measure. Assuming values are in Celsius")
  }
  data[, unit_temp := "degree Celsius"]

  #### Default unit for white cell count is billion per liter
  if(!all(unique(data$unit_wcc) %in% c("billion per liter", NA))){
    warning("White cell count contains an unknown unit of measure. Assuming values are in billion per liter")
  }
  data[, unit_wcc := "billion per liter"]

  #### Default unit for fio2 is ratio
  data[unit_fio2 <= 'percent', max_fio2/100]
  data[unit_fio2 <= 'percent', min_fio2/100]

  if(!all(unique(data$unit_fio2) %in% c("ratio", "percent", NA))){
    warning("fio2 contains an unknown unit of measure. Assuming values are ratio")
  }
  data[, unit_fio2 := "ratio"]

  #### Default unit for pao2 is millimeter mercury column
  data[unit_pao2 <= 'kilopascal', max_pao2*7.50062]
  data[unit_pao2 <= 'kilopascal', min_pao2*7.50062]

  if(!all(unique(data$unit_pao2) %in% c("kilopascal", "millimeter mercury column", NA))){
    warning("pao2 contains an unknown unit of measure. Assuming values are millimeter mercury column")
  }
  data[, unit_pao2 := "millimeter mercury column"]

  #### Default unit for hematocrit is percent
  data[unit_hematocrit <= 'liter per liter', max_hematocrit*100]
  data[unit_hematocrit <= 'liter per liter', min_hematocrit*100]

  data[unit_hematocrit <= 'ratio', max_hematocrit*100]
  data[unit_hematocrit <= 'ratio', min_hematocrit*100]

  if(!all(unique(data$unit_hematocrit) %in% c("liter per liter", "percent", "ratio", NA))){
    warning("hematocrit contains an unknown unit of measure. Assuming values are liter per liter")
  }
  data[, unit_hematocrit := "percent"]

  #### Default unit for sodium is millimole per liter
  data[unit_sodium <= 'millimole per deciliter', max_sodium*10]
  data[unit_sodium <= 'millimole per deciliter', min_sodium*10]

  if(!all(unique(data$unit_sodium) %in% c("millimole per liter", "millimole per deciliter", NA))){
    warning("sodium contains an unknown unit of measure. Assuming values are millimole per liter")
  }
  data[, unit_sodium := "millimole per liter"]

  #### Default unit for potassium is millimole per liter
  data[unit_potassium <= 'millimole per deciliter', max_potassium*10]
  data[unit_potassium <= 'millimole per deciliter', min_potassium*10]

  if(!all(unique(data$unit_potassium) %in% c("millimole per liter", "millimole per deciliter", NA))){
    warning("potassium contains an unknown unit of measure. Assuming values are millimole per liter")
  }
  data[, unit_potassium := "millimole per liter"]

  #### Default unit for creatinine is milligram per deciliter
  data[unit_creatinine <= 'micromole per liter', max_creatinine*0.0113]
  data[unit_creatinine <= 'micromole per liter', min_creatinine*0.0113]

  data[unit_creatinine <= 'millimole per liter', max_creatinine*11.312]
  data[unit_creatinine <= 'millimole per liter', min_creatinine*11.312]

  data[unit_creatinine <= 'milligram per liter', max_creatinine*0.1]
  data[unit_creatinine <= 'milligram per liter', min_creatinine*0.1]

  if(!all(unique(data$unit_creatinine) %in% c("milligram per deciliter", "micromole per liter",
                                              "millimole per liter", "milligram per deciliter", NA))){
    warning("creatinine contains an unknown unit of measure. Assuming values are milligram per deciliter")
  }
  data[, unit_creatinine := "milligram per deciliter"]

  #### Default unit for bicarbonate is millimole per liter
  if(!all(unique(data$unit_bicarbonate) %in% c("millimole per liter", NA))){
    warning("bicarbonate contains an unknown unit of measure. Assuming values are in millimole per liter")
  }
  data[, unit_bicarbonate := "millimole per liter"]

  #### Default for respiratory rate is breaths per minute.
  #### Default unit for heart rate is beats per minute
  #### Default unit for ph is unitless.
  #### Default unit for blood pressure is mmhg.

  data
}


#' Calculates the APACHE II score.
#' Assumes the units of measure have been fixed using the fix_apache_ii_units function.
#' Missing data is handled using normal imputation
#' @param data Dataframe containing physiology variables and units of measure.
#' Should be the output of the get_score_variables function with the 'severity score parameter set to APACHEII"
#'
#' @return A data frame with a variable containing the apache II score calculated.
#' @import data.table
calculate_apache_ii_score <- function(data){

  data <- as.data.table(data)
  # Define the fields requested for full computation
  apache <- c("max_temp", "min_temp", "min_wcc", "max_wcc", "min_map", "max_map",
              "max_fio2", "min_paco2", "min_pao2", "min_hematocrit", "max_hematocrit",
              "min_hr", "max_hr", "min_rr", "max_rr", "min_ph", "max_ph",
              "min_bicarbonate", "max_bicarbonate", "min_sodium", "max_sodium",
              "min_potassium", "max_potassium", "min_gcs", "min_creatinine",
              "max_creatinine", "age")

  ##### The order of conditions is important for all the fields below.
  # Display a warning if fields are missing
  if (length(which(is.na(match(apache, names(data))))) > 0 ){
    apache <- apache[which(!is.na(match(apache, names(data))))]
  }

  #### Temperature. Need to calculate for both min and max to work out which is worse.
  data[, max_temp_ap_ii := 0]
  data[(max_temp < c(30)) | (max_temp > c(40)), max_temp_ap_ii := 4]
  data[(max_temp %between% c(30,31.9)) | (max_temp %between% c(39,40)), max_temp_ap_ii := 3]
  data[max_temp %between% c(32,33.9), max_temp_ap_ii := 2]
  data[(max_temp %between% c(34,35.9)) | (max_temp %between% c(38.5,38.9)), max_temp_ap_ii := 1]
  data[max_temp %between% c(36,38.4), max_temp_ap_ii := 0]

  data[, min_temp_ap_ii := 0]
  data[(min_temp < c(30)) | (min_temp > c(40)), min_temp_ap_ii := 4]
  data[(min_temp %between% c(30,31.9)) | (min_temp %between% c(39,40)), min_temp_ap_ii := 3]
  data[min_temp %between% c(32,33.9), min_temp_ap_ii := 2]
  data[(min_temp %between% c(34,35.9)) | (min_temp %between% c(38.5,38.9)), min_temp_ap_ii := 1]
  data[min_temp %between% c(36,38.4), min_temp_ap_ii := 0]

  ##### White blood cell count
  data[,   min_wcc_ap_ii := 0]
  data[(min_wcc > c(2.999)), min_wcc_ap_ii := 0]
  data[(min_wcc > c(14.999)), min_wcc_ap_ii := 1]
  data[(min_wcc < c(3.000)) | (min_wcc > c(19.999)), min_wcc_ap_ii := 2]
  data[(min_wcc < c(1.000)) | (min_wcc > c(39.999)), min_wcc_ap_ii:= 4]

  data[,   max_wcc_ap_ii := 0]
  data[(max_wcc > c(2.999)), max_wcc_ap_ii := 0]
  data[(max_wcc > c(14.999)), max_wcc_ap_ii := 1]
  data[(max_wcc < c(3.000)) | (max_wcc > c(19.999)), max_wcc_ap_ii := 2]
  data[(max_wcc < c(1.000)) | (max_wcc > c(39.999)), max_wcc_ap_ii:= 4]

  ###### Mean arterial pressure. Calculating MAP first.
  # data[, min_map := min_dbp + 1/3(min_sbp – min_dbp)]
  # data[, max_map := max_dbp + 1/3(max_sbp – max_dbp)]

  data[, min_map_ap_ii := 0]
  data[(min_map < c(50)) | (min_map > c(159)), min_map_ap_ii := 4]
  data[min_map %between% c(130,159), min_map_ap_ii := 3]
  data[(min_map %between% c(50,69)) | (min_map %between% c(110,129)), min_map_ap_ii := 2]
  data[min_map %between% c(70,109), min_map_ap_ii := 0]

  data[, max_map_ap_ii := 0]
  data[(max_map < c(50)) | (max_map > c(159)), max_map_ap_ii := 4]
  data[max_map %between% c(130,159), max_map_ap_ii := 3]
  data[(max_map %between% c(50,69)) | (max_map %between% c(110,129)), max_map_ap_ii := 2]
  data[max_map %between% c(70,109), max_map_ap_ii := 0]

  #### AADO2
  ### Calculating the A-a gradient. The highest value is worst, so picking the ones that match it.
  ### NOTE - should probably get matching fio2, pao2, paco2 instead just getting min and max.
  data[, aado2 := (max_fio2*710) - (min_paco2*1.25) - min_pao2]

  data[, aado2_ap_ii := 0]
  data[max_fio2 >= 0.5 & aado2 >= 500, aado2_ap_ii := 4]
  data[max_fio2 < 0.5 & min_pao2 < 55, aado2_ap_ii := 4]
  data[max_fio2 >= 0.5 & aado2 >= 350 & aado2 < 500, aado2_ap_ii:= 3]
  data[max_fio2 < 0.5 & min_pao2 >= 55 & min_pao2 <= 60, aado2_ap_ii := 3]
  data[max_fio2 >= 0.5 & aado2 >= 200 & aado2 < 350, aado2_ap_ii:= 2]
  data[max_fio2 < 0.5 & min_pao2 > 60 & min_pao2 <= 70, aado2_ap_ii := 1]

  #### Hematocrit
  data[, min_hematocrit_ap_ii := 0]
  data[min_hematocrit > c(29), min_hematocrit_ap_ii := 0]
  data[(min_hematocrit > c(45.9)), min_hematocrit_ap_ii := 1]
  data[(min_hematocrit < c(30)) | (min_hematocrit > c(49.9)), min_hematocrit_ap_ii := 2]
  data[(min_hematocrit < c(20)) | (min_hematocrit > c(59.9)), min_hematocrit_ap_ii := 4]

  data[, max_hematocrit_ap_ii := 0]
  data[max_hematocrit > c(29), max_hematocrit_ap_ii := 0]
  data[(max_hematocrit > c(45.9)), max_hematocrit_ap_ii := 1]
  data[(max_hematocrit < c(30)) | (max_hematocrit > c(49.9)), max_hematocrit_ap_ii := 2]
  data[(max_hematocrit < c(20)) | (max_hematocrit > c(59.9)), max_hematocrit_ap_ii := 4]

  #### heart rate
  data[, min_hr_ap_ii := 0]
  data[min_hr %between% c(70,109), min_hr_ap_ii := 0]
  data[(min_hr %between% c(55,69)) | (min_hr %between% c(110,139)), min_hr_ap_ii := 2]
  data[(min_hr %between% c(40,54)) | (min_hr %between% c(140,179)), min_hr_ap_ii := 3]
  data[(min_hr < c(40)) | (min_hr > c(179)), min_hr_ap_ii := 4]

  data[, max_hr_ap_ii := 0]
  data[max_hr %between% c(70,109), max_hr_ap_ii := 0]
  data[(max_hr %between% c(55,69)) | (max_hr %between% c(110,139)), max_hr_ap_ii := 2]
  data[(max_hr %between% c(40,54)) | (max_hr %between% c(140,179)), max_hr_ap_ii := 3]
  data[(max_hr < c(40)) | (max_hr > c(179)), max_hr_ap_ii := 4]

  #### respiratory rate
  data[, min_rr_ap_ii := 0]
  data[min_rr %between% c(25,34), min_rr_ap_ii := 1]
  data[min_rr %between% c(12,24), min_rr_ap_ii := 0]
  data[min_rr %between% c(10,11), min_rr_ap_ii := 2]
  data[min_rr %between% c(6,9) | (min_rr %between% c(35,49)), min_rr_ap_ii := 3]
  data[(min_rr < c(6)) | (min_rr > c(49)), min_rr_ap_ii := 4]

  data[, max_rr_ap_ii := 0]
  data[max_rr %between% c(25,34), max_rr_ap_ii := 1]
  data[max_rr %between% c(12,24), max_rr_ap_ii := 0]
  data[max_rr %between% c(10,11), max_rr_ap_ii := 2]
  data[max_rr %between% c(6,9) | (max_rr %between% c(35,49)), max_rr_ap_ii := 3]
  data[(max_rr < c(6)) | (max_rr > c(49)), max_rr_ap_ii := 4]

  #### Ph and HCO3
  #### Use Ph if available. If not, use HCO3.
  data[, min_ph_ap_ii := 0]
  data[min_ph > c(7.32), min_ph_ap_ii := 0]
  data[min_ph > c(7.49), min_ph_ap_ii := 1]
  data[min_ph < c(7.33), min_ph_ap_ii := 2]
  data[min_ph < c(7.25) | min_ph > c(7.59), min_ph_ap_ii := 3]
  data[min_ph < c(7.16) | min_ph > c(7.69), min_ph_ap_ii := 4]

  data[, max_ph_ap_ii := 0]
  data[max_ph > c(7.32), max_ph_ap_ii := 0]
  data[max_ph > c(7.49), max_ph_ap_ii := 1]
  data[max_ph < c(7.33), max_ph_ap_ii := 2]
  data[max_ph < c(7.25) | max_ph > c(7.59), max_ph_ap_ii := 3]
  data[max_ph < c(7.16) | max_ph > c(7.69), max_ph_ap_ii := 4]


  ##### bicarbonate Will only get calculated if there is no ph
  data[, min_bicarbonate_ap_ii := 0]
  data[is.na(min_ph) & min_bicarbonate > c(21), min_bicarbonate_ap_ii := 0]
  data[is.na(min_ph) & min_bicarbonate > c(31), min_bicarbonate_ap_ii := 1]
  data[is.na(min_ph) & min_bicarbonate < c(22), min_bicarbonate_ap_ii := 2]
  data[is.na(min_ph) & (min_bicarbonate < c(18) | min_bicarbonate > c(40)), min_bicarbonate_ap_ii := 3]
  data[is.na(min_ph) & (min_bicarbonate < c(16) | min_bicarbonate > c(51)), min_bicarbonate_ap_ii := 4]

  data[, max_bicarbonate_ap_ii := 0]
  data[is.na(max_ph) & max_bicarbonate > c(21), max_bicarbonate_ap_ii := 0]
  data[is.na(max_ph) & max_bicarbonate > c(31), max_bicarbonate_ap_ii := 1]
  data[is.na(max_ph) & max_bicarbonate < c(22), max_bicarbonate_ap_ii := 2]
  data[is.na(max_ph) & (max_bicarbonate < c(18) | max_bicarbonate > c(40)), max_bicarbonate_ap_ii := 3]
  data[is.na(max_ph) & (max_bicarbonate < c(16) | max_bicarbonate > c(51)), max_bicarbonate_ap_ii := 4]

  ##### sodium
  data[, min_sodium_ap_ii := 0]
  data[min_sodium > c(129), min_sodium_ap_ii := 0]
  data[min_sodium > c(149), min_sodium_ap_ii := 1]
  data[min_sodium < c(130) | min_sodium > c(154), min_sodium_ap_ii := 2]
  data[min_sodium < c(120) | min_sodium > c(159), min_sodium_ap_ii := 3]
  data[min_sodium < c(111) | min_sodium > c(179), min_sodium_ap_ii := 4]

  data[, max_sodium_ap_ii := 0]
  data[max_sodium > c(129), max_sodium_ap_ii := 0]
  data[max_sodium > c(149), max_sodium_ap_ii := 1]
  data[max_sodium < c(130) | max_sodium > c(154), max_sodium_ap_ii := 2]
  data[max_sodium < c(120) | max_sodium > c(159), max_sodium_ap_ii := 3]
  data[max_sodium < c(111) | max_sodium > c(179), max_sodium_ap_ii := 4]

  #### potassium
  data[, min_potassium_ap_ii := 0]
  data[min_potassium > c(3.4), min_potassium_ap_ii := 0]
  data[min_potassium < c(3.5) | min_potassium > c(5.4), min_potassium_ap_ii := 1]
  data[min_potassium < c(3), min_potassium_ap_ii := 2]
  data[min_potassium > c(5.9), min_potassium_ap_ii := 3]
  data[min_potassium < c(2.5)  | min_potassium > c(6.9), min_potassium_ap_ii := 4]

  data[, max_potassium_ap_ii := 0]
  data[max_potassium > c(3.4), max_potassium_ap_ii := 0]
  data[max_potassium < c(3.5) | max_potassium > c(5.4), max_potassium_ap_ii := 1]
  data[max_potassium < c(3), max_potassium_ap_ii := 2]
  data[max_potassium > c(5.9), max_potassium_ap_ii := 3]
  data[max_potassium < c(2.5)  | max_potassium > c(6.9), max_potassium_ap_ii := 4]

  #### GCS
  data[, gcs_ap_ii := 15- min_gcs]
  data[is.na(gcs_ap_ii), gcs_ap_ii := 0]

  #### creatinine
  #### Assuming there's no renal failure
  data[, renal_failure := 0]

  data[, min_creat_ap_ii := 0]
  data[min_creatinine >= 3.5, min_creat_ap_ii := 4]
  data[min_creatinine >= 2 & min_creatinine < 3.5, min_creat_ap_ii := 3]
  data[min_creatinine >= 1.5 & min_creatinine < 2, min_creat_ap_ii := 2]
  data[min_creatinine < 0.6, min_creat_ap_ii := 2]
  data[renal_failure == 1, min_creat_ap_ii*2]

  data[, max_creat_ap_ii := 0]
  data[max_creatinine >= 3.5, max_creat_ap_ii := 4]
  data[max_creatinine >= 2 & max_creatinine < 3.5, max_creat_ap_ii := 3]
  data[max_creatinine >= 1.5 & max_creatinine < 2, max_creat_ap_ii := 2]
  data[max_creatinine < 0.6, max_creat_ap_ii := 2]
  data[renal_failure == 1, max_creat_ap_ii*2]

  #### Age.
  data[, age_ap_ii := 0]
  data[age < 44, age_ap_ii := 0]
  data[age %between% c(45, 54), age_ap_ii := 2]
  data[age %between% c(55, 64), age_ap_ii := 3]
  data[age %between% c(65, 74), age_ap_ii := 5]
  data[age > 74, age_ap_ii := 6]

  #### Assuming there are no comorbidities
  data[, comorbid_ap_ii := 0]
  #### Assuming everyone is a planned admission.
  data[, emergency := 0]

  #### chronic health score
  data[, chronic_ap_ii := 0]
  data[comorbid_ap_ii > 0 & emergency == 0, chronic_ap_ii := 2]
  data[comorbid_ap_ii > 0 & emergency == 1, chronic_ap_ii := 5]

  ### Getting the worst score for each component.
  data[, apache_ii_score :=
         pmax(min_temp_ap_ii, max_temp_ap_ii) + pmax(min_wcc_ap_ii, max_wcc_ap_ii) +
         pmax(min_map_ap_ii, max_map_ap_ii) + aado2_ap_ii +
         pmax(min_hematocrit_ap_ii, max_hematocrit_ap_ii) + pmax(min_hr_ap_ii, max_hr_ap_ii) +
         pmax(min_rr_ap_ii, max_rr_ap_ii) + pmax(min_ph_ap_ii, max_ph_ap_ii) +
         pmax(min_bicarbonate_ap_ii, max_bicarbonate_ap_ii) + pmax(min_sodium_ap_ii, max_sodium_ap_ii) +
         pmax(min_potassium_ap_ii, max_potassium_ap_ii) + gcs_ap_ii +
         pmax(min_creat_ap_ii, max_creat_ap_ii) + age_ap_ii + chronic_ap_ii
       ]

  ### Deleting the intermediate variables so the dataset is not too long.
  delete_cols <- c(
    "min_temp_ap_ii", "max_temp_ap_ii", "min_wcc_ap_ii", "max_wcc_ap_ii",
    "min_map_ap_ii", "max_map_ap_ii", "aado2_ap_ii", "min_hematocrit_ap_ii",
    "max_hematocrit_ap_ii", "min_hr_ap_ii", "max_hr_ap_ii", "min_rr_ap_ii", "max_rr_ap_ii",
    "min_ph_ap_ii", "max_ph_ap_ii", "min_bicarbonate_ap_ii", "max_bicarbonate_ap_ii",
    "min_sodium_ap_ii", "max_sodium_ap_ii", "min_potassium_ap_ii", "max_potassium_ap_ii",
    "gcs_ap_ii", "min_creat_ap_ii", "max_creat_ap_ii", "age_ap_ii", "chronic_ap_ii",
    "emergency", "comorbid_ap_ii", "renal_failure")

  data[, (delete_cols) := NULL]
  data
}
