#' Create a connection object to uds.
#' @param host postgres host
#' @param user postgres username
#' @param password postgres password
#'
#' @importFrom DBI dbConnect
#' @importFrom RPostgres Postgres
postgres_connect <- function(host, dbname, port, user, password){
  DBI::dbConnect(
    RPostgres::Postgres(),
    host = host,
    dbname = dbname,
    port = port,
    user = user ,
    password = password)
}

#' Queries the postgres database to get variables required for the severity score specified.
#' Uses visit detail as table containing ICU admission information. If not available, uses visit_occurrence.
#' @param postgres_conn A connection object to the postgres database
#' @param schema The name of the schema you want to query.
#' @param start_date The earliest ICU admission date/datetime. Needs to be in character format.
#' @param end_date As above, but for last date
#' @param min_day The number of days since ICU admission to get physiology data for. Starts with 0
#' @param max_day The number of days since ICU admision to get physiology data for.
#' @param dataset_name Describes which concept codes to select. Can only be CCHIC at the moment.
#' @param severity_score The name of the severity score to calculate. Only APACHE2 at the moment.
#'
#' @import lubridate
#' @import DBI
#' @import dplyr
#' @import glue
#' @import readr
get_score_variables <- function(postgres_conn, schema, start_date, end_date,
                                min_day, max_day, dataset_name, severity_score){

  #### Getting the list of concept IDs required for the score, and creating SQL lines from them.
  concepts <- read_csv(system.file(glue("{dataset_name}_concepts.csv"),
                          package = "SeverityScoresOMOP")) %>%
    filter(score == severity_score) %>%
    mutate(max_query = glue(
      ", MAX(CASE WHEN m.measurement_concept_id = {concept_id} then m.value_as_number END) AS max_{short_name}"),
      min_query = glue(
        ", MIN(CASE WHEN m.measurement_concept_id = {concept_id} then m.value_as_number END) AS min_{short_name}"),
      unit_query = glue(
        ", MIN(CASE WHEN m.measurement_concept_id = {concept_id} then c_unit.concept_name END) AS unit_{short_name}"))

  ## Collapsing all the queries into a string.
  variables_required <- glue(glue_collapse(concepts$max_query, sep = "\n"), "\n",
                      glue_collapse(concepts$min_query, sep = "\n"), "\n",
                      glue_collapse(concepts$unit_query, sep = "\n"))

  #### Importing the rest of the query from the text file.
  raw_sql <- readr::read_file(system.file("physiology_variables.sql",
                                          package = "SeverityScoresOMOP")) %>%
    glue(schema = schema,
         start_date = start_date,
         end_date = end_date,
         min_day = min_day,
         max_day = max_day,
         variables_required = variables_required)

#### Running the query
  data <- dbGetQuery(postgres_conn, raw_sql)
  as_tibble(data)
}

#' Convert units of measure into a format suitable for the APACHE II score calculation
#' Assumes units of measure are encoded in OMOP using the UCUM source vocabulary
#' Throws a warning if the unit of measure is not recognised. Assumes the default unit of measure if not available.
#' @param data Dataframe containing physiology variables and units of measure.
#' Should be the output of the get_score_variables function with the 'severity score parameter set to APACHEII"
#'
#' @return A data frame with the physiology values converted to the default units of measure specified.
#' @import data.table
apache_ii_units <- function(data){

  data <- data.table(data)

  #### Default unit for temperature is celsius
  data[unit_temp <= 'degree Fahrenheit', (max_temp-32)*5/9]
  data[unit_temp <= 'degree Fahrenheit', (min_temp-32)*5/9]

  if(!all(unique(data$unit_temp) %in% c("degree Celsius", "degree Fahrenheit", NA))){
    warning("Temperature contains an unknown unit of measure. Assuming values are in Celsius")
  }

  #### Default unit for white cell count is billion per liter
  if(!all(unique(data$unit_wcc) %in% c("billion per liter", NA))){
    warning("White cell count contains an unknown unit of measure. Assuming values are in billion per liter")
  }

  #### Default unit for fio2 is ratio
  data[unit_fio2 <= 'percent', max_fio2/100]
  data[unit_fio2 <= 'percent', min_fio2/100]

  if(!all(unique(data$unit_fio2) %in% c("ratio", "percent", NA))){
    warning("fio2 contains an unknown unit of measure. Assuming values are ratio")
  }

  #### Default unit for pao2 is millimeter mercury column
  data[unit_pao2 <= 'kilopascal', max_pao2*7.50062]
  data[unit_pao2 <= 'kilopascal', min_pao2*7.50062]

  if(!all(unique(data$unit_pao2) %in% c("kilopascal", "millimeter mercury column", NA))){
    warning("pao2 contains an unknown unit of measure. Assuming values are millimeter mercury column")
  }

  #### Default unit for hematocrit is percent
  data[unit_hematocrit <= 'liter per liter', max_hematocrit*100]
  data[unit_hematocrit <= 'liter per liter', min_hematocrit*100]

  data[unit_hematocrit <= 'ratio', max_hematocrit*100]
  data[unit_hematocrit <= 'ratio', min_hematocrit*100]

  if(!all(unique(data$unit_hematocrit) %in% c("liter per liter", "percent", "ratio", NA))){
    warning("hematocrit contains an unknown unit of measure. Assuming values are liter per liter")
  }

  #### Default unit for sodium is millimole per liter
  data[unit_sodium <= 'millimole per deciliter', max_sodium*10]
  data[unit_sodium <= 'millimole per deciliter', min_sodium*10]

  if(!all(unique(data$unit_sodium) %in% c("millimole per liter", "millimole per deciliter", NA))){
    warning("sodium contains an unknown unit of measure. Assuming values are millimole per liter")
  }

  #### Default unit for potassium is millimole per liter
  data[unit_potassium <= 'millimole per deciliter', max_potassium*10]
  data[unit_potassium <= 'millimole per deciliter', min_potassium*10]

  if(!all(unique(data$unit_potassium) %in% c("millimole per liter", "millimole per deciliter", NA))){
    warning("potassium contains an unknown unit of measure. Assuming values are millimole per liter")
  }

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

  #### Default unit for bicarbonate is millimole per liter
  if(!all(unique(data$unit_bicarbonate) %in% c("millimole per liter", NA))){
    warning("bicarbonate contains an unknown unit of measure. Assuming values are in millimole per liter")
  }

  #### Default for respiratory rate is breaths per minute.
  #### Default unit for heart rate is beats per minute
  #### Default unit for ph is unitless.
  #### Default unit for blood pressure is mmhg.

  data
}
