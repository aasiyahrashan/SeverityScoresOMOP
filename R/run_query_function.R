#' Build the pasted_visits SQL string based on visit_mode.
#'
#' Encapsulates the logic for choosing and rendering the correct SQL file
#' for ICU stay identification. Called internally by \code{get_score_variables}.
#'
#' @param visit_mode One of "paste", "raw", "ground_truth".
#' @param dialect SQL dialect.
#' @param paste_gap_hours Gap threshold for paste mode.
#' @param ground_truth A \code{gt_config} object (ground_truth mode only).
#'
#' @return A character string containing the rendered SQL for the visit identification CTEs.
#'
#' @importFrom readr read_file
#' @importFrom SqlRender render
#' @importFrom glue glue
build_visit_sql <- function(visit_mode, dialect,
                            paste_gap_hours = 6,
                            ground_truth = NULL) {

  if (visit_mode == "ground_truth") {

    # Hospital cols are optional — render as full SQL expression or NULL literal.
    gt_hosp_adm_expr <- if (!is.null(ground_truth$gt_hospital_admission_col)) {
      glue("gt.{ground_truth$gt_hospital_admission_col}")
    } else {
      "NULL"
    }
    gt_hosp_dis_expr <- if (!is.null(ground_truth$gt_hospital_discharge_col)) {
      glue("gt.{ground_truth$gt_hospital_discharge_col}")
    } else {
      "NULL"
    }

    # Partially render the SQL. @ground_truth_values is left unrendered —
    # it gets filled per batch in the main loop.
    pasted_visits_sql <-
      read_file(system.file("ground_truth_admission_details.sql",
                            package = "SeverityScoresOMOP")) %>%
      render(
        gt_hospital_admission_expr = gt_hosp_adm_expr,
        gt_hospital_discharge_expr = gt_hosp_dis_expr
      )

  } else if (visit_mode == "raw") {

    pasted_visits_sql <-
      read_file(system.file("raw_visit_details.sql",
                            package = "SeverityScoresOMOP"))

  } else {
    # Default: "paste" mode (original behaviour)
    pasted_visits_sql <-
      read_file(system.file("paste_disjoint_icu_visits.sql",
                            package = "SeverityScoresOMOP")) %>%
      render(paste_gap_hours = paste_gap_hours)
  }

  pasted_visits_sql
}


#' Queries a database to get variables required for a specified severity score.
#' Assumes visit detail table contains ICU admission information. If not
#' available, uses visit_occurrence.
#'
#' @param conn A connection object to a database
#' @param dialect A dialect supported by SQLRender
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
#' @param concepts_file_path
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
#' @param visit_mode How to determine ICU stays. One of:
#' \describe{
#'   \item{"paste"}{(Default) Stitches disjoint visit_detail rows together if the gap
#'     between them is less than \code{paste_gap_hours}. This is the original behaviour.}
#'   \item{"raw"}{Uses visit_detail rows as-is, without any stitching.
#'     Use when visit_detail is already clean.}
#'   \item{"ground_truth"}{Uses an external ground truth table to define ICU stays.
#'     Requires a \code{gt_config} object via the \code{ground_truth} parameter.}
#' }
#' @param paste_gap_hours Numeric. Only used when \code{visit_mode = "paste"}.
#' Maximum gap in hours between consecutive visit_detail rows before they are
#' treated as separate ICU stays. Defaults to 6.
#' @param ground_truth A \code{gt_config} object created by
#' \code{\link{ground_truth_config}}. Only required when
#' \code{visit_mode = "ground_truth"}. Bundles the ground truth dataframe,
#' joining schema details, and column name mappings into a single object.
#' @param run_validation Logical. Only used when \code{visit_mode = "ground_truth"}.
#' If TRUE (default), runs a bidirectional check comparing ground truth patients
#' against OMOP ICU patients. Set to FALSE to skip this check (e.g. to avoid a
#' slow visit_detail scan on large databases).
#' @param verbose Logical. If TRUE, prints the rendered SQL queries to the
#' console via \code{message()}. Defaults to FALSE.
#' @param dry_run Logical. If TRUE, builds and returns the fully rendered SQL
#' queries without executing them or connecting to the database (beyond
#' ground truth ID resolution if in ground_truth mode). Returns a named list
#' with elements \code{main_sql}, \code{gcs_sql} (or NULL), and
#' \code{person_ids_sql} (the query used to find patient IDs, or NULL in
#' ground truth mode). Useful for debugging and testing.
#'
#' @returns If \code{dry_run = FALSE} (default): A tibble containing one row per combination of `person`, `visit_occurrence`, `visit_detail`, and `cadence`.
#' The `time_in_icu` variable is the amount of time spent in ICU. The unit depends on cadence. If cadence is 24, the unit will be a day.
#' If cadence is 1, the unit will be an hour, and so on.
#' The columns returned will be named using the short_names specified in the `mapping_path` file.
#' In ground truth mode, the original ground truth columns (e.g. external IDs) are joined
#' back onto the result for debugging and downstream joins.
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
                                batch_size = 10000,
                                visit_mode = "paste",
                                paste_gap_hours = 6,
                                ground_truth = NULL,
                                run_validation = TRUE,
                                verbose = FALSE,
                                dry_run = FALSE) {

  # --- Validate dialect argument ---
  supported_dialects <- c("postgresql", "sql server")
  if (!tolower(dialect) %in% supported_dialects) {
    stop("dialect must be one of: ", paste(supported_dialects, collapse = ", "),
         ". Got: '", dialect, "'")
  }

  # --- Validate visit_mode argument ---
  if (!visit_mode %in% c("paste", "raw", "ground_truth")) {
    stop("visit_mode must be one of: 'paste', 'raw', 'ground_truth'")
  }

  # --- Validate paste_gap_hours ---
  if (visit_mode == "paste" && (!is.numeric(paste_gap_hours) || paste_gap_hours <= 0)) {
    stop("paste_gap_hours must be a positive number.")
  }

  # --- Validate ground truth parameters ---
  if (visit_mode == "ground_truth") {
    if (is.null(ground_truth) || !inherits(ground_truth, "gt_config")) {
      stop("When visit_mode = 'ground_truth', a gt_config object must be ",
           "passed via the 'ground_truth' parameter. ",
           "Create one with ground_truth_config().")
    }

    # Validate the ground truth dataframe
    validate_ground_truth(ground_truth)

    if (run_validation && !dry_run) {
      message("Validating ground truth against OMOP...")
      validation_result <- validate_ground_truth_vs_omop(
        conn = conn,
        gt_config = ground_truth,
        schema = schema,
        start_date = start_date,
        end_date = end_date,
        dialect = dialect
      )
    }
  }

  # Creating age query
  age_query <- age_query(age_method)

  # --- Select the appropriate SQL for visit identification ---
  pasted_visits_sql <- build_visit_sql(
    visit_mode = visit_mode,
    dialect = dialect,
    paste_gap_hours = paste_gap_hours,
    ground_truth = ground_truth
  )

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
  if (nrow(gcs_concepts) > 0) {
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

    # Note: paste_gap_hours and hospital col params are already rendered into
    # pasted_visits_sql. In ground truth mode, @ground_truth_values is left
    # unrendered here — it gets filled per batch in the loop below.

    # Print query if verbose
    if (verbose) {
      message("=== GCS Query ===")
      message(gcs_raw_sql)
    }
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
      age_query = age_query,
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

  # Note: paste_gap_hours and hospital col params are already rendered into
  # pasted_visits_sql. In ground truth mode, @ground_truth_values is left
  # unrendered here — it gets filled per batch in the loop below.

  if (verbose) {
    message("=== Main Query ===")
    message(raw_sql)
  }

  # --- Dry run: return rendered SQL without executing ---
  if (dry_run) {
    # Render with placeholder person IDs so the SQL is complete and runnable.
    example_ids <- "1, 2, 3"

    if (visit_mode == "ground_truth") {
      ts_type <- if (tolower(dialect) == "postgresql") "TIMESTAMP" else "DATETIME"
      example_gt_values <- glue(
        "(1, 100, CAST('2000-01-01 00:00:00' AS {ts_type}), ",
        "CAST('2000-01-02 00:00:00' AS {ts_type}))")
      main_sql_example <- render(raw_sql, ground_truth_values = example_gt_values)
      gcs_sql_example <- if (nrow(gcs_concepts) > 0) {
        render(gcs_raw_sql, ground_truth_values = example_gt_values)
      } else {
        NULL
      }
    } else {
      main_sql_example <- render(raw_sql, person_ids = example_ids)
      gcs_sql_example <- if (nrow(gcs_concepts) > 0) {
        render(gcs_raw_sql, person_ids = example_ids)
      } else {
        NULL
      }
    }

    person_ids_sql <- if (visit_mode != "ground_truth") {
      glue("SELECT DISTINCT person_id
    FROM {schema}.visit_detail
    WHERE visit_detail_concept_id IN (581379, 32037)
    AND COALESCE(visit_detail_start_datetime,
    visit_detail_start_date) <= '{end_date}'")
    } else {
      NULL
    }

    message("Dry run complete. Returning SQL without executing.")
    return(list(
      main_sql = main_sql_example,
      gcs_sql = gcs_sql_example,
      person_ids_sql = person_ids_sql,
      # Also return the unrendered templates (with @person_ids/@ground_truth_values
      # still present) for inspection.
      main_sql_template = raw_sql,
      gcs_sql_template = if (nrow(gcs_concepts) > 0) gcs_raw_sql else NULL
    ))
  }

  # --- Get person_ids for batching ---
  resolved_gt <- NULL  # Only populated in ground_truth mode
  if (visit_mode == "ground_truth") {
    # Resolve all ground truth rows to OMOP IDs up front.
    # This returns a dataframe with person_id, visit_occurrence_id,
    # icu_admission_datetime, icu_discharge_datetime.
    resolved_gt <- resolve_ground_truth_ids(
      conn = conn,
      gt_config = ground_truth,
      dialect = dialect
    )

    person_ids <- unique(resolved_gt$person_id)

  } else {
    # Original batching: get person_ids from visit_detail
    person_sql <- glue("
    SELECT DISTINCT person_id
    FROM {schema}.visit_detail
    WHERE visit_detail_concept_id IN (581379, 32037)
                       AND COALESCE(visit_detail_start_datetime,
                       visit_detail_start_date) <= '{end_date}' \n")

    person_ids <- dbGetQuery(conn, person_sql)$person_id
  }

  id_batches <- split(person_ids, ceiling(seq_along(person_ids)/batch_size))


  # Running the query in batches
  results <- vector("list", length(id_batches))
  gcs_results <- vector("list", length(id_batches))
  for (i in seq_along(id_batches)) {

    person_ids_batch <- id_batches[[i]]

    # Print batch number
    print(glue("Main query - Running batch {i} of {length(id_batches)}"))

    # In ground truth mode, render the VALUES clause for this batch.
    # In paste/raw modes, render person_ids as before.
    if (visit_mode == "ground_truth") {
      gt_values <- build_ground_truth_values_clause(resolved_gt, person_ids_batch, dialect)
      raw_sql_batch <- raw_sql %>%
        render(ground_truth_values = gt_values)
    } else {
      raw_sql_batch <- raw_sql %>%
        render(person_ids = glue_collapse(person_ids_batch, sep = ", "))
    }

    # Timing
    start_time <- Sys.time()

    # Getting the data
    results[[i]] <- dbGetQuery(conn, raw_sql_batch)

    # Running GCS query
    if(nrow(gcs_concepts) > 0) {
      # Print batch number
      print(glue("GCS query - Running batch {i} of {length(id_batches)}"))

      if (visit_mode == "ground_truth") {
        gcs_raw_sql_batch <- gcs_raw_sql %>%
          render(ground_truth_values = gt_values)
      } else {
        gcs_raw_sql_batch <- gcs_raw_sql %>%
          render(person_ids = glue_collapse(person_ids_batch, sep = ", "))
      }

      gcs_results[[i]] <- dbGetQuery(conn, gcs_raw_sql_batch)
    }

    # Print times
    end_time <- Sys.time()
    message("Total execution time: ", round(difftime(end_time, start_time, units = "secs"), 2), " seconds")
  }

  data <- bind_rows(results)
  gcs_data <- bind_rows(gcs_results)

  ### Don't like joining here, but not much choice.
  if (nrow(gcs_concepts) > 0) {
    data <- left_join(data,
                      gcs_data,
                      by = c("person_id",
                             "icu_admission_datetime",
                             "time_in_icu"))
  }

  # In ground truth mode, join the original ground truth columns back onto the
  # result so that external IDs (e.g. IcuStayRegistryKey) are available for
  # debugging and downstream joins. The join uses person_id + icu_admission_datetime
  # which should be unique per ICU stay in the ground truth.
  if (visit_mode == "ground_truth" && !is.null(resolved_gt)) {
    # Select only the original ground truth columns (drop the standardised ones
    # that are already in the OMOP result to avoid duplicates).
    gt_cols_to_join <- resolved_gt[,
                                   !(colnames(resolved_gt) %in% c("visit_occurrence_id",
                                                                  "icu_discharge_datetime")),
                                   drop = FALSE]
    # Deduplicate in case multiple ground truth rows mapped to the same
    # person_id + icu_admission_datetime (shouldn't happen, but be safe).
    gt_cols_to_join <- gt_cols_to_join[
      !duplicated(gt_cols_to_join[, c("person_id", "icu_admission_datetime")]), ]

    data <- left_join(data,
                      gt_cols_to_join,
                      by = c("person_id", "icu_admission_datetime"))
  }

  as_tibble(data)
}
