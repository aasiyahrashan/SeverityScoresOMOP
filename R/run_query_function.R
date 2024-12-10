#' Queries a database to get variables required for a specified severity score.
#' Assumes visit detail table contains ICU admission information. If not
#' available, uses visit_occurrence.
#'
#' @param conn A connection object to a database
#' @param dialect dialect A dialect supported by SQLRender
#' @param schema The name of the schema you want to query.
#' @param start_datetime
#' The earliest ICU admission date/datetime. Needs to be in character format.
#' @param end_datetime
#' As above, but for last date
#' @param first_window An integer >= 0
#' First timepoint (depending on cadence argument)
#' since ICU admission to get physiology data for, starting with 0.
#' Eg, if cadence is 24, 0 will be the first day. If cadence is 1, 0 will be the first hour.
#' @param last_window An integer >= 0
#' Last timepoint (depending on cadence argument)
#' since ICU admission to get physiology data for.
#' @param mapping_path
#' Path to the custom *_concepts.tsv file containing score to OMOP mappings.
#' Should match the example_concepts.tsv file format.
#' @param severity_score
#' A vector including the names of the severity scores to calculate.
#' Currently supports "APACHE II" and "SOFA".
#' (TODO, remove this argument and allow all variables in the csv file to be read in.)
#' @param age_method
#' Either 'year_only' or 'dob'. Decides if age is calculated from year of birth or full DOB
#' Default is 'dob'
#' @param cadence A number > 0. Represents the unit of time used for each row per patient returned.
#' The argument is specified in multiples of an hour. Eg, '24' represents a day, '1' represents an hour,
#' '0.5' is 30 minutes. Defaults to '24' so a day is returned.
#' Use in this argument in conjuction with the `first_window` and `last_window` arguments above to decide
#' how much data to extract.
#' Eg, a `cadence` of 24 with `first_window = 0` and `last_window = 3` will return data from the
#' first 4 days of admission, with one row per day.
#' @param window_start_point Only applicable if `cadence = 24`. Decides how to define days.
#'  Either `calendar_date` or `icu_admission_time`.
#' Option 1 (the default) uses the calendar date only, with day 0 being the date of admission to ICU.
#' If this option is chosen, cadence must be `24`.
#' Option 2 (should be used for CC-HIC or EHR data) divides observations into 24 hour windows from ICU admission time.
#'
#' @returns A tibble containing one row per combination of `person`, `visit_occurrence`, `visit_detail`, and `cadence`.
#' The `time_in_icu` variable is the amount of time spent in ICU. The unit depends on cadence. If cadence is 24, the unit will be a day.
#' If cadence is 1, the unit will be an hour, and so on.
#' The columns returned will be named using the short_names specified in the `mapping_path` file
#'
#' @import lubridate
#' @import DBI
#' @import dplyr
#' @import glue
#' @import readr
#' @importFrom stringr str_detect
#' @importFrom SqlRender translate render
#' @importFrom readr read_file
#' @export
get_score_variables <- function(conn, dialect, schema,
                                start_date, end_date,
                                first_window, last_window,
                                concepts_file_path,
                                severity_score,
                                age_method = "dob",
                                cadence = 24,
                                window_start_point = "calendar_date") {
  # Editing the date variables to keep explicit single quote for SQL
  start_date <- single_quote(start_date)
  end_date <- single_quote(end_date)

  # Creating age query
  age_query <- age_query(age_method)

  # Admission information
  raw_sql <- read_file(
    system.file("admission_details.sql", package = "SeverityScoresOMOP")) %>%
    render(schema             = schema,
           age_query          = age_query,
           start_date         = start_date,
           end_date           = end_date) %>%
    translate(tolower(dialect))
  adm_details <- dbGetQuery(conn, raw_sql)t

  # Getting list of all variable names
  variable_names <- read_delim(file = system.file("variable_names.csv",
                                                  package = "SeverityScoresOMOP"))
  # Getting the list of concept IDs required and creating SQL queries from them.
  concepts <- read_delim(file = concepts_file_path) %>%
    # Filtering for the scores required.
    filter(str_detect(score, paste(severity_score, collapse = "|"))) %>%
    mutate(concept_id = as.character(concept_id))

  # Older versions of the concepts files may not have the additional filter variables.
  # Adding them here.
  if (!"additional_filter_variable_name" %in% colnames(concepts)){
    concepts <- concepts %>%
      mutate(additional_filter_variable_name = NA,
             additional_filter_value = NA)
  }

  # Making sure each short name only has additional filter variable.
  if(concepts %>%
     group_by(short_name) %>%
     summarise(n = n_distinct(additional_filter_variable_name)) %>%
     filter(n > 1) %>%
     nrow() > 0) {
    stop("There is more than one `additional_filter_variable_name` per `short_name`. Please fix this.")
  }

  # Making sure that string search concepts don't have extra filters.
  if(any(!is.na(concepts %>%
                filter(omop_variable == "concept_name") %>%
                pull(concept_id_value)))) {
    stop("A line with `omop_variable` set to `concept_name` has concept_id_value filled in.
         Either delete the value in `concept_id_value`, or change the `omop_variable` to
         something that contains a concept_id")
  }

  # This collapsed list is required for the main sql query.
  all_required_variables <- concepts %>%
    filter(table %in% c("Measurement", "Observation", "Condition",
                        "Procedure", "Visit Detail", "Device", "Drug")) %>%
    mutate(short_name =
             case_when(omop_variable == "value_as_concept_id" |
                         omop_variable == "concept_name" |
                         is.na(omop_variable) ~
                         glue("count_{short_name}"),
                       omop_variable == "value_as_number" ~
                         glue("min_{short_name}, max_{short_name}, unit_{short_name}"))) %>%
    distinct(short_name) %>%
    pull(.) %>%
    toString(.) %>%
    glue(",", .)

  # GCS needs a separate query if it uses concept IDs.
  gcs_concepts <- concepts %>%
    filter((short_name %in% c("gcs_eye", "gcs_motor", "gcs_verbal") &
              omop_variable == "value_as_concept_id"))

  if (nrow(gcs_concepts > 0)) {
    # The SQL file currently has the GCS LOINC concepts hardcoded.
    # I plan to construct it from here when I have time.
    raw_sql <-
      read_file(system.file("gcs_if_stored_as_concept.sql",
                            package = "SeverityScoresOMOP")) %>%
      render(schema = schema,
             start_date = start_date,
             end_date = end_date,
             first_window = first_window,
             last_window = last_window,
             window_measurement = window_query(window_start_point,
                                               "measurement_datetime",
                                               "measurement_date", cadence)) %>%
      translate(tolower(dialect))

    # Running the query
    gcs_data <- dbGetQuery(conn, raw_sql)
  }

  # Constructing with query for each table.
  with_queries_per_table <- concepts %>%
    distinct(table) %>%
    mutate(with_query = with_query(concepts, table, variable_names,
                                   window_start_point, cadence))

  # Constructing end join query for each table.
  with_queries_per_table <- with_queries_per_table %>%
    # Need to get alias of the previous table for the timing join, if it exists.
    left_join(variable_names %>% select(table, alias),
              by = "table") %>%
    mutate(prev_alias = lag(alias)),
    mutate(end_join_query = end_join_query(table, variable_names,
                                           prev_alias))

  # Combining each query type into a string
  all_with_queries <- glue_collapse(with_queries_per_table$with_query,
                                    sep = "\n")
  all_end_join_queries <- glue_collapse(with_queries_per_table$end_join_query,
                                        sep = "\n")
  all_time_in_icu <- glue_collapse(with_queries_per_table$alias,
                                   sep = ", ")

  # Importing the physiology data query and substituting variables
  raw_sql <- read_file(
    system.file("physiology_variables.sql", package = "SeverityScoresOMOP")) %>%
    render(schema             = schema,
           first_window       = first_window,
           last_window        = last_window,
           all_required_variables = all_required_variables,
           drug_join = translate_drug_join(dialect),
           window_measurement = window_query(window_start_point,
                                             "measurement_datetime",
                                             "measurement_date", cadence),
           window_observation = window_query(window_start_point,
                                             "observation_datetime",
                                             "observation_date", cadence),
           window_condition = window_query(window_start_point,
                                           "condition_start_datetime",
                                           "condition_start_date", cadence),
           window_procedure = window_query(window_start_point,
                                           "procedure_datetime",
                                           "procedure_date", cadence),
           window_device = window_query(window_start_point,
                                        "device_exposure_start_datetime",
                                        "device_exposure_start_date", cadence),
           window_drug_start = window_query(window_start_point,
                                        "drug_exposure_start_datetime",
                                        "drug_exposure_start_date", cadence),
           window_drug_end = window_query(window_start_point,
                                        "drug_exposure_end_datetime",
                                        "drug_exposure_end_date", cadence),
           measurement_variables = variables_query(concepts, "Measurement",
                                                   "measurement_concept_id"),
           observation_variables = variables_query(concepts, "Observation",
                                                   "observation_concept_id",
                                                   "value_as_concept_id",
                                                   table_id_var =  "observation_id"),
           condition_variables = variables_query(concepts, "Condition",
                                                 "condition_concept_id",
                                                 table_id_var = "condition_occurrence_id"),
           procedure_variables = variables_query(concepts, "Procedure",
                                                 "procedure_concept_id",
                                                 table_id_var = "procedure_occurrence_id"),
           device_variables = variables_query(concepts, "Device",
                                              "device_concept_id",
                                              table_id_var = "device_exposure_id"),
           drug_variables = variables_query(concepts, "Drug",
                                            "concept_name",
                                            table_id_var = "drug_exposure_id"),
           visit_detail_variables = visit_detail_variables,
           drug_string_search_expression = string_search_expression(concepts, "Drug")) %>%
    translate(tolower(dialect)) %>%
    render(start_date         = start_date,
           end_date           = end_date,)

  # I really don't understand this, but sqlrender translates the start_date value for
  # this query only, with an extra, incorrect cast. Replacing the parameter with the value
  # later to prevent this.

  #### Running the query
  data <- dbGetQuery(conn, raw_sql)

  ### Don't like joining here, but not much choice.
  if (nrow(gcs_concepts > 0)) {
    data <- left_join(data,
                      gcs_data,
                      by = c("person_id",
                             "visit_occurrence_id",
                             "visit_detail_id",
                             "time_in_icu"))
  }

  # Joining to ICU admission details to get all patients
  data <- left_join(adm_details, data,
                    by = c("person_id",
                           "visit_occurrence_id",
                           "visit_detail_id",
                           "icu_admission_datetime"))

  as_tibble(data)
}
