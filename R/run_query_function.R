#' Queries a database to get variables required for a specified severity score.
#' Assumes visit detail table contains ICU admission information. If not
#' available, uses visit_occurrence.
#'
#' @param conn A connection object to a database
#' @param dialect dialect A dialect supported by SQLRender
#' @param schema The name of the schema you want to query.
#' @param start_date
#' The earliest ICU admission date/datetime. The filter is inclusive of the value specified. Needs to be in character format.
#' @param end_date
#' As above, but for last date. The filter is inclusive of the value specified.
#' @param first_window An integer
#' First timepoint (depending on cadence argument) to get physiology data for.
#' Eg, if cadence is 24, 0 will be the first day of the ICU visit. If cadence is 1, 0 will be the first hour.
#' Negative values can get data from before the ICU stay, as long as the data is within the hospital visit.
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
#' @param batch_size Defaults to 10000, refers to number of ICU patients in whole dataset, since filtering on time is not possible.
#'
#' @returns A tibble containing one row per combination of `person`, `visit_occurrence`, `visit_detail`, and `cadence`.
#' The `time_in_icu` variable is the amount of time spent in ICU. The unit depends on cadence. If cadence is 24, the unit will be a day.
#' If cadence is 1, the unit will be an hour, and so on.
#' The columns returned will be named using the short_names specified in the `mapping_path` file
#'
#' @import dplyr
#' @importFrom DBI dbGetQuery dbDisconnect
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
                                window_start_point = "calendar_date",
                                batch_size = 10000) {
  # Creating age query
  age_query <- age_query(age_method)

  # Processing the ICU admission and discharge query.
  # This pastes visit details together.
  pasted_visits_sql <-
    read_file(system.file("paste_disjoint_icu_visits.sql",
                          package = "SeverityScoresOMOP")) %>%
    render(age_query = age_query)

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
                filter(omop_variable %in% c("concept_name", "concept_code")) %>%
                pull(concept_id_value)))) {
    stop("A line with `omop_variable` set to `concept_name` or `concept_code`
    has concept_id_value filled in.
         Either delete the value in `concept_id_value`, or change the `omop_variable` to
         something that contains a concept_id")
  }

  # GCS needs a separate query if it uses concept IDs.
  gcs_concepts <- concepts %>%
    filter((short_name %in% c("gcs_eye", "gcs_motor", "gcs_verbal") &
              omop_variable == "value_as_concept_id"))
  concepts <- concepts %>%
    filter(!(short_name %in% c("gcs_eye", "gcs_motor", "gcs_verbal") &
              omop_variable == "value_as_concept_id"))

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

  # Constructing the GCS query
  if (nrow(gcs_concepts > 0)) {
    # The SQL file currently has the GCS LOINC concepts hardcoded.
    # I plan to construct it from here when I have time.
    gcs_raw_sql <-
      read_file(system.file("gcs_if_stored_as_concept.sql",
                            package = "SeverityScoresOMOP")) %>%
      render(
        pasted_visits = pasted_visits_sql,
        schema = schema,
        first_window = first_window,
        last_window = last_window,
        window_measurement = window_query(window_start_point,
                                          "measurement_datetime",
                                          "measurement_date", cadence)) %>%
      translate(tolower(dialect)) %>%
      # There is a bug in SQL render which means dates need to be rendered after translating
      render(start_date = single_quote(start_date),
             end_date = single_quote(end_date))

    # Running the query
    cat("Printing GCS query \n")
    cat(gcs_raw_sql)
  }

  # Constructing the main query
  # Importing the physiology data query and substituting variables
  raw_sql <- read_file(
    system.file("physiology_variables.sql",
                package = "SeverityScoresOMOP")) %>%
    render(
      pasted_visits = pasted_visits_sql,
      first_window = first_window,
      last_window = last_window,
      schema = schema,
      all_with_queries = all_with_queries,
      all_end_join_queries = all_end_join_queries,
      all_time_in_icu = all_id_vars(end_join_queries$alias, "time_in_icu"),
      all_person_id = all_id_vars(end_join_queries$alias, "person_id"),
      all_icu_admission_datetime = all_id_vars(end_join_queries$alias, "icu_admission_datetime"),
      all_required_variables = all_required_variables) %>%
    translate(tolower(dialect)) %>%
    # There is a bug in SQL render which means dates need to be rendered after translating
    render(start_date = single_quote(start_date),
           end_date = single_quote(end_date))

  cat("Printing main query")
  cat(raw_sql)

  # Batching person_ids
  # -- ICU stay or critical care
  # And the visit had to have started before the end date filter.
  person_sql <- glue("
  SELECT DISTINCT person_id
  FROM {schema}.visit_detail
  WHERE visit_detail_concept_id IN (581379, 32037)
                     AND COALESCE(visit_detail_start_datetime,
                     visit_detail_start_date) <= '{end_date}' \n")

  person_ids <- dbGetQuery(conn, person_sql)$person_id
  id_batches <- split(person_ids, ceiling(seq_along(person_ids)/batch_size))


  # Running the query in batches
  results <- vector("list", length(id_batches))
  gcs_results <- vector("list", length(id_batches))
  for (i in seq_along(id_batches)) {

    person_ids_batch <- id_batches[[i]]

    # Print batch number
    print(glue("Main query - Running batch {i} of {length(id_batches)}"))

    raw_sql_batch <- raw_sql %>%
      render(person_ids = glue_collapse(person_ids_batch, sep = ", "))

    # Getting the data
    results[[i]] <- dbGetQuery(conn, raw_sql_batch)

    # Running GCS query
    if(nrow(gcs_concepts) > 0) {
      # Print batch number
      print(glue("GCS query - Running batch {i} of {length(id_batches)}"))

      gcs_raw_sql_batch <- gcs_raw_sql %>%
        render(person_ids = glue_collapse(person_ids_batch, sep = ", "))

      gcs_results[[i]] <- dbGetQuery(conn, gcs_raw_sql_batch)
    }
  }

  data <- bind_rows(results)
  gcs_data <- bind_rows(gcs_results)

  ### Don't like joining here, but not much choice.
  if (nrow(gcs_concepts > 0)) {
    data <- left_join(data,
                      gcs_data,
                      by = c("person_id",
                             "icu_admission_datetime",
                             "time_in_icu"))
  }

  as_tibble(data)
}
