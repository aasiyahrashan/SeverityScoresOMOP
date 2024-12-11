#' Queries a database to get variables required for a specified severity score.
#' Assumes visit detail table contains ICU admission information. If not
#' available, uses visit_occurrence.
#'
#' @param conn A connection object to a database
#' @param dialect dialect A dialect supported by SQLRender
#' @param schema The name of the schema you want to query.
#' @param start_datetime
#' The earliest ICU admission date/datetime. The filter is inclusive of the value specified. Needs to be in character format.
#' @param end_datetime
#' As above, but for last date. The filter is inclusive of the value specified.
#' @param first_window An integer >= 0
#' First timepoint (depending on cadence argument)
#' since ICU admission to get physiology data for, starting with 0.
#' Eg, if cadence is 24, 0 will be the first day. If cadence is 1, 0 will be the first hour.
#' The filter is inclusive of the value specified.
#' @param last_window An integer >= 0
#' Last timepoint (depending on cadence argument) since ICU admission
#' to get physiology data for. The filter is inclusive of the value specified.
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
#' @import dplyr
#' @importFrom DBI dbGetQuery
#' @importFrom glue glue glue_collapse single_quote
#' @importFrom stringr str_detect
#' @importFrom SqlRender translate render
#' @importFrom readr read_file read_delim
#' @importFrom purrr accumulate map
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

  # Getting list of all variable names
  variable_names <- read_delim(file = system.file("variable_names.csv",
                                                  package = "SeverityScoresOMOP"))

  # Getting the list of concept IDs required and creating SQL queries from them.
  concepts <- read_delim(file = concepts_file_path) %>%
    # Filtering for the scores required.
    filter(str_detect(score, paste(severity_score, collapse = "|"))) %>%
    filter(table %in% c("Measurement", "Observation", "Condition",
                        "Procedure", "Visit Detail", "Device", "Drug")) %>%
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

  # Creating age query
  age_query <- age_query(age_method)

  # Constructing with query for each table.
  with_queries_per_table <- concepts %>%
    distinct(table) %>%
    pull(table) %>%
    map(., ~ ifelse(.x == "Drug",
                    drug_with_query(concepts, variable_names = variable_names,
                                    window_start_point, cadence,
                                    dialect),
                    with_query(concepts,.x, variable_names,
                               window_start_point = window_start_point,
                               cadence)))

  # Constructing end join query for each table.
  end_join_queries <-
    concepts %>%
    distinct(table) %>%
    # Need to get alias of the previous table for the timing join, if it exists.
    left_join(variable_names %>% select(table, alias),
              by = "table") %>%
    # Pasting all previous aliases together so I can use it in a coalesce for a join.
    mutate(prev_alias = accumulate(glue("{alias}.placeholder"),
                                       ~ glue("{.x}, {.y}"))) %>%
    mutate(prev_alias = lag(prev_alias)) %>%
    # end_join_query is not vectorised
    rowwise() %>%
    mutate(end_join_query = end_join_query(table, variable_names,
                                           prev_alias)) %>%
    ungroup()

  # Combining each query type into a string
  all_with_queries <- glue_collapse(with_queries_per_table,
                                    sep = "\n")
  all_end_join_queries <- glue_collapse(end_join_queries$end_join_query,
                                        sep = "\n")
  # All required variables for the queries
  all_required_variables <- all_required_variables_query(concepts)

  # Importing the physiology data query and substituting variables
  raw_sql <- read_file(
    system.file("physiology_variables.sql",
                package = "SeverityScoresOMOP")) %>%
    render(
      age_query = age_query,
      all_with_queries = all_with_queries,
      all_end_join_queries = all_end_join_queries,
      all_time_in_icu = all_id_vars(end_join_queries$alias, "time_in_icu"),
      all_person_id = all_id_vars(end_join_queries$alias, "person_id"),
      all_visit_occurrence_id = all_id_vars(end_join_queries$alias, "visit_occurrence_id"),
      all_visit_detail_id = all_id_vars(end_join_queries$alias, "visit_detail_id"),
      all_required_variables = all_required_variables) %>%
    # Don't know why the translate function adds an unnecessary CAST statement to the dates.
    # So not translating that part.
    translate(tolower(dialect)) %>%
    render(
      first_window = first_window,
      last_window = last_window,
      schema = schema,
      start_date = start_date,
      end_date = end_date)

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

  as_tibble(data)
}
