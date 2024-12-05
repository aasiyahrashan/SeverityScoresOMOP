#' Create a connection object to on OMOP compliant database.
#' @param driver driver eg. "SQL Server", "PostgreSQL", etc.
#' @param host host/server name
#' @param dbname name of database in server
#' @param port database port
#' @param user username
#' @param password password
#' @importFrom DBI dbConnect
#' @importFrom RPostgres Postgres
#' @importFrom odbc odbc
#' @importFrom SqlRender translate render
#' @export
omop_connect <-
  function(driver, host, dbname, port, user, password) {
    if (driver == "PostgreSQL") {
      dbConnect(
        Postgres(),
        host = host,
        dbname = dbname,
        port = port,
        user = user ,
        password = password
      )
    } else {
      dbConnect(
        odbc(),
        driver = driver,
        server = host,
        database = dbname,
        uid = user,
        pwd = password
      )
    }
  }

#' Internal function called in `get_score_variables`.
#' Builds the windowing query based on variable names in each table.
window_query <- function(window_start_point, time_variable, date_variable, cadence,
                         dialect){
  ifelse(
    window_start_point == "calendar_date",
    # Returns one row per day based on calendar date rather than admission time.
    glue(" DATEDIFF(dd, adm.icu_admission_datetime, COALESCE(t.{time_variable}, t.{date_variable}))"),
    # Uses admission date as starting point, returns rows based on cadence argument.
    glue(" FLOOR(DATEDIFF(MINUTE, adm.icu_admission_datetime, COALESCE(t.{time_variable}, t.{date_variable})) / ({cadence} * 60))")) %>%
    translate(tolower(dialect))
}

#' Internal function called in `get_score_variables`.
#' Builds a regex for string searches
string_search_expression <- function(concepts, table_name) {
  # This collapsed list is used for string queries
  string_search_expression <-
    concepts %>%
    filter(table == table_name &
             omop_variable == "concept_name") %>%
    # It's possible for strings to represent the same variable, so grouping here.
    group_by(short_name) %>%
    summarise(
      concept_id = glue("'%",
                        glue_collapse(tolower(unique(concept_id)),
                                      sep = "%|%"),
                        "%'")) %>%
    pull(concept_id)
  string_search_expression
}
#' Internal function called in `get_score_variables`.
#' Builds the 'variable required' query for each table
variables_query <- function(concepts, table_name,
                                     concept_id_var_name,
                                     value_as_concept_id_var = "",
                                     table_id_var = ""){

  variables_required = ""

  # Numeric variables
  numeric_concepts <-
    concepts %>%
    filter(table == table_name) %>%
    filter(omop_variable == "value_as_number") %>%
    #### GCS can sometimes be stored as a concept ID instead
    #### of a number. These need a separate query.
    filter(!(short_name %in% c("gcs_eye", "gcs_motor", "gcs_verbal") &
               omop_variable == "value_as_concept_id")) %>%
    # It's possible for multiple concept IDs to represent the same variable, so grouping here.
    group_by(short_name, omop_variable, additional_filter_variable_name) %>%
    summarise(
      # This is slightly odd, but just makes sure we don't duplicate concept
      # IDs in cases where we're selecting specific values
      # The query returns the number of rows matching the concept IDs provided.
      concept_id = ifelse(length(unique(concept_id)) == 1,
                          as.character(unique(concept_id)),
                          glue("'",
                               glue_collapse(unique(concept_id), sep = "', '"),
                               "'")),
      additional_filter_value = glue(
        "'",
        glue_collapse(additional_filter_value, sep = "', '"),
        "'"
      )
    ) %>%
    mutate(
      additional_filter_query =
        if_else(!is.na(additional_filter_variable_name),
                glue("AND {additional_filter_variable_name}
              IN ({additional_filter_value})"), "", ""),
      max_query = glue(
        ", MAX(CASE WHEN {concept_id_var_name} IN ({concept_id})
           {additional_filter_query}
                    THEN {omop_variable}
               END) AS max_{short_name}"
      ),
      min_query = glue(
        ", MIN(CASE WHEN {concept_id_var_name} IN ({concept_id})
           {additional_filter_query}
                    THEN {omop_variable}
               END) AS min_{short_name}"
      ),
      unit_query = glue(
        ", MIN(CASE WHEN {concept_id_var_name} IN ({concept_id})
           {additional_filter_query}
                    THEN c_unit.concept_name
               END) AS unit_{short_name}"
      ))

  # Non-numeric variables if concept ID provided. Returns counts.
  non_numeric_concepts <-
    concepts %>%
    filter(table == table_name &
             (omop_variable == "value_as_concept_id" |
                is.na(omop_variable))) %>%
    # It's possible for multiple concept IDs to represent the same variable, so grouping here.
    group_by(short_name, omop_variable, additional_filter_variable_name) %>%
    summarise(
      # This is slightly odd, but just makes sure we don't duplicate concept
      # IDs in cases where we're selecting specific values
      # The query returns the number of rows matching the concept IDs provided.
      concept_id = glue("'",
                        glue_collapse(unique(concept_id), sep = "', '"),
                        "'"),
      concept_id_value = glue(
        "'",
        glue_collapse(concept_id_value, sep = "', '"),
        "'"
      ),
      additional_filter_value = glue(
        "'",
        glue_collapse(additional_filter_value, sep = "', '"),
        "'"
      )
    ) %>%
    # Building the query in separate elements, depending on which conditions are filled
    mutate(
      value_as_concept_id_query = if_else(
        omop_variable == "value_as_concept_id",
        glue("AND {value_as_concept_id_var}
              IN ({concept_id_value})"), "", ""),
      additional_filter_query =
        if_else(!is.na(additional_filter_variable_name),
                glue("AND {additional_filter_variable_name}
              IN ({additional_filter_value})"), "", ""),
      count_query = glue(
        ", COUNT ( CASE WHEN {concept_id_var_name} IN ({concept_id})
           {value_as_concept_id_query}
           {additional_filter_query}
           THEN {table_id_var}
           END ) AS count_{short_name}"))

  # Non-numeric variables if no concept ID provided.
  # Does a string search on concept name.
  # Returns counts.
  non_numeric_string_search_concepts <-
    concepts %>%
    filter(table == table_name &
             omop_variable == "concept_name") %>%
    # It's possible for strings to represent the same variable, so grouping here.
    group_by(short_name, additional_filter_variable_name) %>%
    summarise(
      # This is slightly odd, but just makes sure we don't duplicate concept
      # IDs in cases where we're selecting specific values
      # The query returns the number of rows matching the concept IDs provided.
      concept_id = glue("'%",
                        glue_collapse(tolower(unique(concept_id)),
                                      sep = "%|%"),
                        "%'"),
      additional_filter_value = glue(
        "'",
        glue_collapse(additional_filter_value, sep = "', '"),
        "'"
      )
    ) %>%
    # Building the query in separate elements, depending on which conditions are filled
    mutate(
      additional_filter_query =
        if_else(!is.na(additional_filter_variable_name),
                glue("AND {additional_filter_variable_name}
              IN ({additional_filter_value})"), "", ""),
      count_query = glue(
        ", COUNT ( CASE WHEN LOWER({concept_id_var_name}) SIMILAR TO {concept_id})
           {additional_filter_query}
           THEN {table_id_var}
           END ) AS count_{short_name}"))

  # Collapsing the queries into strings.
  variables_required <-
    glue(glue_collapse(numeric_concepts$max_query, sep = "\n"),
         "\n",
         glue_collapse(numeric_concepts$min_query, sep = "\n"),
         "\n",
         glue_collapse(numeric_concepts$unit_query, sep = "\n"),
         "\n",
         glue_collapse(non_numeric_concepts$count_query, sep = "\n"),
         "\n",
         glue_collapse(non_numeric_string_search_concepts$count_query, sep = "\n"),
         "\n"
         )

  # Return query
  variables_required
}


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

  # Checking arguments
  if (!(window_start_point %in% c("calendar_date", "icu_admission_time"))) {
    stop("Error: window_start_point must be either 'calendar_date' or 'icu_admission_time'")
  }

  if (window_start_point == "calendar_date" & cadence != 24) {
    stop("Error: Since window_start_point is set to 'calendar_date', cadence must be 24.
         Either edit the window_start_point to 'icu_admission_time', or edit the cadence to 24.")
  }

  if(!cadence > 0){
    stop("cadence must be a number > 0'")
  }

  # First getting admission information
  # Age query
  if (!(age_method %in% c("dob", "year_only"))) {
  stop("Error: age_method must be either 'dob' or 'year_only'")
}
  age_query <- ifelse(
    age_method == "dob",
  " DATEDIFF(yyyy,
			  COALESCE(p.birth_datetime, DATEFROMPARTS(p.year_of_birth, COALESCE(p.month_of_birth, '06'),
          COALESCE(p.day_of_birth, '01'))),
			  COALESCE(vd.visit_detail_start_datetime, vd.visit_detail_start_date,
        vo.visit_start_datetime, vo.visit_start_date)) as age",
  " YEAR(COALESCE(vd.visit_detail_start_datetime, vd.visit_detail_start_date,
                   vo.visit_start_datetime, vo.visit_start_date)) - p.year_of_birth AS age") %>%
  translate(tolower(dialect))

  # Admission information
  raw_sql <- read_file(
    system.file("admission_details.sql", package = "SeverityScoresOMOP")) %>%
    translate(tolower(dialect)) %>%
    render(schema             = schema,
           age_query          = age_query,
           start_date         = start_date,
           end_date           = end_date)

  adm_details <- dbGetQuery(conn, raw_sql)

  # Getting the list of concept IDs required and creating SQL queries from them.
  concepts <- read_delim(file = concepts_file_path) %>%
    # Filtering for the scores required.
    filter(str_detect(score, paste(severity_score, collapse = "|")))

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
  if(!concepts %>%
     filter(omop_variable == "concept_name") %>%
     pull(concept_id_value) %>%
     all(is.na(.))) {
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
      translate(tolower(dialect)) %>%
      render(schema = schema,
             start_date = start_date,
             end_date = end_date,
             first_window = first_window,
             last_window = last_window,
             window_measurement = window_measurement)

    # Running the query
    gcs_data <- dbGetQuery(conn, raw_sql)
  }

  # Admisison type stored in the visit detail also needs a separate query,
  # though it's run with everything else.

    visit_detail_concepts <- concepts %>%
      filter(table == "Visit Detail" & short_name == "emergency_admission")

    # Initialize count query strings
    visit_detail_variables = ""

    # Emergency admissions from the visit detail table
    if (nrow(visit_detail_concepts) > 0) {
      emergency_admission_concept <- visit_detail_concepts$concept_id
      visit_detail_variables <- glue(
        ",COUNT( CASE
         WHEN t.visit_detail_source_concept_id = {emergency_admission_concept}
         THEN t.visit_detail_id
     END ) AS count_emergency_admission "
      )
    }

  # Importing the physiology data query and substituting variables
  raw_sql <- read_file(
    system.file("physiology_variables.sql", package = "SeverityScoresOMOP")) %>%
    translate(tolower(dialect)) %>%
    render(schema              = schema,
           start_date         = start_date,
           end_date           = end_date,
           first_window       = first_window,
           last_window        = last_window,
           all_required_variables = all_required_variables,
           window_measurement = window_query(window_start_point,
                                              "measurement_datetime",
                                              "measurement_date", cadence,
                                              dialect),
           window_observation = window_query(window_start_point,
                                             "observation_datetime",
                                             "observation_date", cadence,
                                             dialect),
           window_condition = window_query(window_start_point,
                                           "condition_start_datetime",
                                           "condition_start_date", cadence,
                                           dialect),
           window_procedure = window_query(window_start_point,
                                           "procedure_datetime",
                                           "procedure_date", cadence,
                                           dialect),
           window_device = window_query(window_start_point,
                                        "device_exposure_start_datetime",
                                        "device_exposure_start_date", cadence,
                                        dialect),
           window_drug_start = window_query(window_start_point,
                                            "drug_exposure_start_datetime",
                                            "drug_exposure_start_date", cadence,
                                            dialect),
           window_drug_end = window_query(window_start_point,
                                          "drug_exposure_end_datetime",
                                          "drug_exposure_end_date", cadence,
                                           dialect),
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
           drug_string_search_expression = string_search_expression(concepts, "Drug"))

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
