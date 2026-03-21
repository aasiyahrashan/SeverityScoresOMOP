#' Build the pasted_visits SQL string based on visit_mode.
#'
#' @param visit_mode One of "paste", "raw", "ground_truth".
#' @param dialect SQL dialect.
#' @param paste_gap_hours Gap threshold for paste mode.
#' @param ground_truth A \code{gt_config} object (ground_truth mode only).
#'
#' @return A character string containing the rendered SQL for the visit
#'   identification CTEs.
#'
#' @importFrom readr read_file
#' @importFrom SqlRender render
#' @importFrom glue glue
build_visit_sql <- function(visit_mode, dialect,
                            paste_gap_hours = 6,
                            ground_truth = NULL) {

  if (visit_mode == "ground_truth") {

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

    read_file(system.file("ground_truth_admission_details.sql",
                          package = "SeverityScoresOMOP")) %>%
      render(
        gt_hospital_admission_expr = gt_hosp_adm_expr,
        gt_hospital_discharge_expr = gt_hosp_dis_expr
      )

  } else if (visit_mode == "raw") {

    read_file(system.file("raw_visit_details.sql",
                          package = "SeverityScoresOMOP"))

  } else {
    read_file(system.file("paste_disjoint_icu_visits.sql",
                          package = "SeverityScoresOMOP")) %>%
      render(paste_gap_hours = paste_gap_hours)
  }
}


#' Render a SQL template with common parameters and translate to dialect.
#'
#' Applies schema, age_query, window parameters, translates, then renders
#' date parameters (must happen after translate due to SqlRender bug).
#'
#' @param sql Raw SQL string with \@ parameters.
#' @param schema OMOP schema name.
#' @param age_query Rendered age expression.
#' @param first_window First time window.
#' @param last_window Last time window.
#' @param dialect SQL dialect.
#' @param start_date Start date string.
#' @param end_date End date string.
#'
#' @return Fully rendered and translated SQL string.
#' @keywords internal
render_and_translate <- function(sql, schema, age_query,
                                 first_window, last_window,
                                 dialect, start_date, end_date) {
  sql %>%
    render(schema = schema,
           age_query = age_query,
           first_window = first_window,
           last_window = last_window) %>%
    translate(tolower(dialect)) %>%
    # SqlRender bug: dates must be rendered after translation
    render(start_date = single_quote(start_date),
           end_date = single_quote(end_date))
}


#' Pivot long-format SQL results to wide format.
#'
#' Takes long-format rows (one per person/stay/window/concept_id) and
#' creates min_{short_name}, max_{short_name}, unit_{short_name} columns.
#'
#' Handles multiple concept_ids per short_name by aggregating (min of mins,
#' max of maxes, first non-NA unit).
#'
#' Handles additional_filter_variable_name disambiguation: when the same
#' concept_id maps to different short_names based on a filter column, only
#' rows matching the filter values are assigned to each short_name.
#'
#' @param long_data A dataframe from a long-format query.
#' @param concept_map A data.table from build_concept_map().
#'
#' @return A data.table in wide format, or an empty data.table.
#' @import data.table
#' @keywords internal
pivot_long_to_wide <- function(long_data, concept_map) {

  if (is.null(long_data) || nrow(long_data) == 0) {
    return(data.table())
  }

  long_dt <- as.data.table(long_data)
  long_dt[, concept_id := as.character(concept_id)]

  # Many-to-many merge: a concept_id can map to multiple short_names
  # (when disambiguated by additional filters).
  mapped <- merge(long_dt, concept_map,
                  by = "concept_id",
                  allow.cartesian = TRUE)

  # Apply additional filter logic
  if ("additional_filter_variable_name" %in% names(mapped)) {
    has_filter <- !is.na(mapped$additional_filter_variable_name)
    if (any(has_filter)) {
      keep <- rep(TRUE, nrow(mapped))
      for (i in which(has_filter)) {
        filter_col <- mapped$additional_filter_variable_name[i]
        filter_vals <- mapped$additional_filter_values[[i]]
        if (!all(is.na(filter_vals)) && filter_col %in% names(mapped)) {
          keep[i] <- mapped[[filter_col]][i] %in% filter_vals
        }
      }
      mapped <- mapped[keep]
    }
  }

  if (nrow(mapped) == 0) return(data.table())

  # Aggregate per (person, stay, window, short_name)
  key_cols <- c("person_id", "icu_admission_datetime", "time_in_icu",
                "short_name")
  agg <- mapped[,
                .(min_val = min(min_val, na.rm = TRUE),
                  max_val = max(max_val, na.rm = TRUE),
                  unit_name = unit_name[!is.na(unit_name)][1]),
                by = key_cols]

  agg[is.infinite(min_val), min_val := NA_real_]
  agg[is.infinite(max_val), max_val := NA_real_]

  if (nrow(agg) == 0) return(data.table())

  # Pivot to wide
  id_cols <- c("person_id", "icu_admission_datetime", "time_in_icu")

  wide_min <- dcast(agg,
                    person_id + icu_admission_datetime + time_in_icu ~ short_name,
                    value.var = "min_val", fun.aggregate = min, fill = NA)
  wide_max <- dcast(agg,
                    person_id + icu_admission_datetime + time_in_icu ~ short_name,
                    value.var = "max_val", fun.aggregate = max, fill = NA)
  wide_unit <- dcast(agg,
                     person_id + icu_admission_datetime + time_in_icu ~ short_name,
                     value.var = "unit_name",
                     fun.aggregate = function(x) x[!is.na(x)][1],
                     fill = NA)

  short_names <- setdiff(names(wide_min), id_cols)
  setnames(wide_min, short_names, paste0("min_", short_names))
  setnames(wide_max, short_names, paste0("max_", short_names))
  setnames(wide_unit, short_names, paste0("unit_", short_names))

  result <- merge(wide_min, wide_max, by = id_cols)
  result <- merge(result, wide_unit, by = id_cols)
  result
}


#' Merge all physiology dataframes onto an admission spine.
#'
#' Takes the admission details (one row per ICU stay) and a list of
#' physiology dataframes (each with person_id, icu_admission_datetime,
#' time_in_icu as keys). Builds a complete time_in_icu grid from all
#' physiology data, then merges everything together and RIGHT JOINs
#' to admissions.
#'
#' @param admissions data.table of admission details (one row per ICU stay).
#' @param physiology_dfs Named list of data.tables with physiology data.
#'   Each must have person_id, icu_admission_datetime, time_in_icu columns.
#'
#' @return A data.table with admission details + all physiology columns,
#'   one row per (person, stay, time_in_icu).
#' @import data.table
#' @keywords internal
merge_admissions_and_physiology <- function(admissions, physiology_dfs) {

  admissions <- as.data.table(admissions)
  key_cols <- c("person_id", "icu_admission_datetime", "time_in_icu")

  # Filter to non-empty dataframes
  non_empty <- Filter(function(df) !is.null(df) && nrow(df) > 0, physiology_dfs)

  if (length(non_empty) == 0) {
    # No physiology data — return admissions with no time_in_icu rows.
    # This mirrors the old behaviour where spine was empty.
    return(admissions[, time_in_icu := NA_integer_])
  }

  # Build the spine: UNION of all (person_id, icu_admission_datetime, time_in_icu)
  spine_parts <- lapply(non_empty, function(df) {
    as.data.table(df)[, ..key_cols]
  })
  spine <- unique(rbindlist(spine_parts, use.names = TRUE))

  # LEFT JOIN each physiology df onto the spine
  result <- spine
  for (df in non_empty) {
    dt <- as.data.table(df)
    # Only join columns that aren't already in result (except keys)
    new_cols <- setdiff(names(dt), names(result))
    join_cols <- c(key_cols, new_cols)
    if (length(new_cols) > 0) {
      result <- merge(result, dt[, ..join_cols], by = key_cols, all.x = TRUE)
    }
  }

  # RIGHT JOIN to admissions: keep all admissions, including those with no
  # physiology data. Filter out rows where time_in_icu is NA (matches old
  # WHERE spine.time_in_icu IS NOT NULL behaviour).
  adm_key <- c("person_id", "icu_admission_datetime")
  result <- merge(admissions, result, by = adm_key, all.x = TRUE, allow.cartesian = TRUE)
  result <- result[!is.na(time_in_icu)]

  # Order to match old output
  setorder(result, person_id, icu_admission_datetime, time_in_icu)
  result
}


#' Normalise concepts dataframe columns for backward compatibility.
#'
#' Different concepts files use different column names. This function
#' standardises them.
#'
#' @param concepts A dataframe from reading a concepts CSV.
#' @return The normalised dataframe.
#' @keywords internal
normalise_concepts_columns <- function(concepts) {

  if (!"concept_id_value" %in% colnames(concepts)) {
    concepts$concept_id_value <- NA
  }
  if (!"name_of_value" %in% colnames(concepts)) {
    concepts$name_of_value <- NA
  }

  # Handle column name inconsistency between concepts files
  has_afvv <- "additional_filter_variable_value" %in% colnames(concepts)
  has_afv <- "additional_filter_value" %in% colnames(concepts)
  has_afvn <- "additional_filter_variable_name" %in% colnames(concepts)

  if (!has_afvn) {
    concepts$additional_filter_variable_name <- NA
  }

  if (has_afvv && !has_afv) {
    concepts$additional_filter_value <- concepts$additional_filter_variable_value
  } else if (has_afv && !has_afvv) {
    concepts$additional_filter_variable_value <- concepts$additional_filter_value
  } else if (!has_afv && !has_afvv) {
    concepts$additional_filter_value <- NA
    concepts$additional_filter_variable_value <- NA
  }

  concepts
}


#' Queries a database to get variables required for a specified severity score.
#'
#' Extracts physiology and clinical data from OMOP CDM tables, using
#' separate optimised queries per table. Numeric variables use long-format
#' SQL aggregation with R-side pivoting for performance. Count-based and
#' drug variables use CASE WHEN aggregation.
#'
#' @param conn A connection object to a database
#' @param dialect A dialect supported by SQLRender
#' @param schema The name of the schema you want to query.
#' @param start_date The earliest ICU admission date/datetime (inclusive, character).
#' @param end_date The latest ICU admission date/datetime (inclusive, character).
#' @param first_window First time window (inclusive).
#' @param last_window Last time window (inclusive).
#' @param concepts_file_path Path to the concepts CSV.
#' @param severity_score Character vector of severity score names to filter by.
#' @param age_method Either 'dob' or 'year_only'. Default 'dob'.
#' @param cadence Hours per time window. Default 24.
#' @param window_start_point 'calendar_date' or 'icu_admission_time'. Default 'calendar_date'.
#' @param batch_size Number of patients per batch. Default 10000.
#' @param visit_mode 'paste', 'raw', or 'ground_truth'. Default 'paste'.
#' @param paste_gap_hours Max gap hours for paste mode. Default 6.
#' @param ground_truth A gt_config object for ground_truth mode.
#' @param run_validation Run ground truth validation. Default TRUE.
#' @param verbose Print SQL queries. Default FALSE.
#' @param dry_run Return SQL without executing. Default FALSE.
#'
#' @return A tibble with one row per (person, visit, time_in_icu).
#'
#' @import dplyr
#' @import data.table
#' @importFrom DBI dbGetQuery dbExecute
#' @importFrom glue glue glue_collapse single_quote
#' @importFrom stringr str_detect
#' @importFrom SqlRender translate render
#' @importFrom readr read_file read_delim
#' @importFrom purrr map
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

  # --- Validate arguments ---
  supported_dialects <- c("postgresql", "sql server")
  if (!tolower(dialect) %in% supported_dialects) {
    stop("dialect must be one of: ", paste(supported_dialects, collapse = ", "),
         ". Got: '", dialect, "'")
  }
  if (!visit_mode %in% c("paste", "raw", "ground_truth")) {
    stop("visit_mode must be one of: 'paste', 'raw', 'ground_truth'")
  }
  if (visit_mode == "paste" && (!is.numeric(paste_gap_hours) || paste_gap_hours <= 0)) {
    stop("paste_gap_hours must be a positive number.")
  }
  if (visit_mode == "ground_truth") {
    if (is.null(ground_truth) || !inherits(ground_truth, "gt_config")) {
      stop("When visit_mode = 'ground_truth', a gt_config object must be ",
           "passed via the 'ground_truth' parameter. ",
           "Create one with ground_truth_config().")
    }
    validate_ground_truth(ground_truth)
    if (run_validation && !dry_run) {
      message("Validating ground truth against OMOP...")
      validate_ground_truth_vs_omop(
        conn = conn, gt_config = ground_truth, schema = schema,
        start_date = start_date, end_date = end_date, dialect = dialect
      )
    }
  }

  # --- Build shared components ---
  age_qry <- age_query(age_method)
  pasted_visits_sql <- build_visit_sql(visit_mode, dialect,
                                       paste_gap_hours, ground_truth)

  variable_names <- read_delim(
    file = system.file("variable_names.csv", package = "SeverityScoresOMOP"),
    show_col_types = FALSE)

  concepts <- read_delim(file = concepts_file_path, show_col_types = FALSE) %>%
    filter(str_detect(score, paste(severity_score, collapse = "|"))) %>%
    filter(table %in% c("Measurement", "Observation", "Condition",
                        "Procedure", "Visit Detail", "Device", "Drug")) %>%
    mutate(concept_id = as.character(concept_id)) %>%
    normalise_concepts_columns()

  # Validate concepts
  if (concepts %>%
      group_by(short_name) %>%
      summarise(n = n_distinct(additional_filter_variable_name), .groups = "drop") %>%
      filter(n > 1) %>%
      nrow() > 0) {
    stop("More than one `additional_filter_variable_name` per `short_name`. Please fix this.")
  }
  if (any(!is.na(concepts %>%
                  filter(omop_variable %in% c("concept_name", "concept_code")) %>%
                  pull(concept_id_value)))) {
    stop("A row with omop_variable = 'concept_name' or 'concept_code' has ",
         "concept_id_value filled in. Remove it or change the omop_variable.")
  }

  # GCS stored as concept IDs needs a separate query
  gcs_concepts <- concepts %>%
    filter(short_name %in% c("gcs_eye", "gcs_motor", "gcs_verbal") &
             omop_variable == "value_as_concept_id")
  concepts <- concepts %>%
    filter(!(short_name %in% c("gcs_eye", "gcs_motor", "gcs_verbal") &
               omop_variable == "value_as_concept_id"))

  # Build concept map for R-side pivoting
  concept_map <- build_concept_map(concepts)

  # --- Build admission query ---
  admission_sql <- read_file(
    system.file("physiology_variables.sql",
                package = "SeverityScoresOMOP")) %>%
    render(pasted_visits = pasted_visits_sql,
           schema = schema,
           age_query = age_qry) %>%
    translate(tolower(dialect)) %>%
    render(start_date = single_quote(start_date),
           end_date = single_quote(end_date))

  # --- Build per-table physiology queries ---
  # Each table can produce up to two queries: long (numeric) and count.
  # Drug table gets its own builder.

  physiology_tables <- concepts %>%
    filter(table != "Drug") %>%
    filter(!(table == "Visit Detail" & short_name != "emergency_admission")) %>%
    distinct(table) %>%
    pull(table)

  query_templates <- list()  # Named list: key -> {sql, type}

  for (tbl in physiology_tables) {
    tbl_concepts <- concepts %>%
      filter(table == tbl) %>%
      filter(!(table == "Visit Detail" & short_name != "emergency_admission"))

    vn <- variable_names %>% filter(table == tbl)

    # Long-format query for numeric variables
    long_sql <- build_long_query(tbl_concepts, tbl, variable_names,
                                 pasted_visits_sql, window_start_point, cadence)
    if (long_sql != "") {
      long_rendered <- render_and_translate(
        long_sql, schema, age_qry, first_window, last_window,
        dialect, start_date, end_date)
      query_templates[[paste0(vn$alias, "_long")]] <- list(
        sql = long_rendered, type = "long")
    }

    # Count query for non-numeric variables
    count_sql <- build_count_query(tbl_concepts, tbl, variable_names,
                                   pasted_visits_sql, window_start_point, cadence)
    if (count_sql != "") {
      count_rendered <- render_and_translate(
        count_sql, schema, age_qry, first_window, last_window,
        dialect, start_date, end_date)
      query_templates[[paste0(vn$alias, "_counts")]] <- list(
        sql = count_rendered, type = "counts")
    }
  }

  # Drug query
  drug_sql <- build_drug_query(concepts, variable_names, pasted_visits_sql,
                               window_start_point, cadence, dialect)
  if (drug_sql != "") {
    drug_rendered <- render_and_translate(
      drug_sql, schema, age_qry, first_window, last_window,
      dialect, start_date, end_date)
    query_templates[["drg"]] <- list(sql = drug_rendered, type = "counts")
  }

  # GCS query
  gcs_sql <- NULL
  if (nrow(gcs_concepts) > 0) {
    gcs_sql <- read_file(
      system.file("gcs_if_stored_as_concept.sql",
                  package = "SeverityScoresOMOP")) %>%
      render(pasted_visits = pasted_visits_sql,
             schema = schema,
             first_window = first_window,
             last_window = last_window,
             window_measurement = window_query(window_start_point,
                                               "measurement_datetime",
                                               "measurement_date", cadence)) %>%
      translate(tolower(dialect)) %>%
      render(start_date = single_quote(start_date),
             end_date = single_quote(end_date))
  }

  # --- Verbose output ---
  if (verbose) {
    message("=== Admission Query ===")
    message(admission_sql)
    for (nm in names(query_templates)) {
      message(glue("=== {nm} Query ==="))
      message(query_templates[[nm]]$sql)
    }
    if (!is.null(gcs_sql)) {
      message("=== GCS Query ===")
      message(gcs_sql)
    }
  }

  # --- Dry run ---
  if (dry_run) {
    example_ids <- "1, 2, 3"

    render_batch <- function(sql, gt_mode) {
      if (gt_mode) {
        ts_type <- if (tolower(dialect) == "postgresql") "TIMESTAMP" else "DATETIME"
        render(sql, ground_truth_values = glue(
          "(1, 100, CAST('2000-01-01 00:00:00' AS {ts_type}), ",
          "CAST('2000-01-02 00:00:00' AS {ts_type}))"))
      } else {
        render(sql, person_ids = example_ids)
      }
    }

    gt_mode <- visit_mode == "ground_truth"

    message("Dry run complete. Returning SQL without executing.")
    return(list(
      admission_sql = render_batch(admission_sql, gt_mode),
      query_templates = lapply(query_templates, function(q) {
        list(sql = render_batch(q$sql, gt_mode), type = q$type)
      }),
      gcs_sql = if (!is.null(gcs_sql)) render_batch(gcs_sql, gt_mode) else NULL,
      concept_map = concept_map,
      # Unrendered templates for inspection
      admission_sql_template = admission_sql,
      gcs_sql_template = gcs_sql,
      query_sql_templates = lapply(query_templates, function(q) q$sql)
    ))
  }

  # --- Get person_ids for batching ---
  resolved_gt <- NULL
  if (visit_mode == "ground_truth") {
    resolved_gt <- resolve_ground_truth_ids(
      conn = conn, gt_config = ground_truth, dialect = dialect)
    person_ids <- unique(resolved_gt$person_id)
  } else {
    person_sql <- glue("
    SELECT DISTINCT person_id
    FROM {schema}.visit_detail
    WHERE visit_detail_concept_id IN (581379, 32037)
      AND COALESCE(visit_detail_start_datetime,
                   visit_detail_start_date) <= '{end_date}' \n")
    person_ids <- dbGetQuery(conn, person_sql)$person_id
  }

  id_batches <- split(person_ids, ceiling(seq_along(person_ids) / batch_size))

  # --- Execute in batches ---
  # Process each batch incrementally: fetch, pivot, and append to accumulator
  # lists that hold only the final-shape data. Raw long-format rows are
  # discarded immediately after pivoting so they don't accumulate in memory.
  batch_admissions <- vector("list", length(id_batches))
  batch_physiology <- vector("list", length(id_batches))

  # Helper: drop a temp table safely (dialect-aware).
  drop_temp_table <- function(conn, table_name, dialect) {
    sql <- if (tolower(dialect) == "sql server") {
      glue("IF OBJECT_ID('tempdb..#{table_name}') IS NOT NULL DROP TABLE #{table_name}")
    } else {
      glue("DROP TABLE IF EXISTS {table_name}")
    }
    tryCatch(dbExecute(conn, sql), error = function(e) NULL)
  }

  # Helper: create/refresh the person_batch temp table for a set of IDs.
  create_person_batch <- function(conn, person_ids_batch, dialect) {
    drop_temp_table(conn, "person_batch", dialect)
    dbExecute(conn, translate("CREATE TEMP TABLE person_batch (person_id INT)",
                              targetDialect = tolower(dialect)))
    id_rows <- glue_collapse(glue("({person_ids_batch})"), sep = ", ")
    dbExecute(conn, glue("INSERT INTO person_batch VALUES {id_rows}"))
    dbExecute(conn, if (tolower(dialect) == "sql server") {
      "UPDATE STATISTICS person_batch"
    } else {
      "ANALYZE person_batch"
    })
  }

  for (i in seq_along(id_batches)) {

    person_ids_batch <- id_batches[[i]]
    message(glue("Running batch {i} of {length(id_batches)}"))

    create_person_batch(conn, person_ids_batch, dialect)

    # Render batch-specific SQL (fills @person_ids or @ground_truth_values)
    render_for_batch <- function(sql) {
      if (visit_mode == "ground_truth") {
        gt_values <- build_ground_truth_values_clause(
          resolved_gt, person_ids_batch, dialect)
        render(sql, ground_truth_values = gt_values)
      } else {
        render(sql, person_ids = glue_collapse(person_ids_batch, sep = ", "))
      }
    }

    start_time <- Sys.time()

    # --- Admission details ---
    batch_admissions[[i]] <- dbGetQuery(conn, render_for_batch(admission_sql))

    # --- Physiology queries: fetch, pivot if long, collect ---
    # Each query result is processed immediately. Raw long-format rows
    # are discarded after pivoting — only the wide result is kept.
    this_batch_physiology <- list()

    for (nm in names(query_templates)) {
      raw <- dbGetQuery(conn, render_for_batch(query_templates[[nm]]$sql))
      if (is.null(raw) || nrow(raw) == 0) next

      if (query_templates[[nm]]$type == "long") {
        # Pivot immediately, discard raw long-format data
        pivoted <- pivot_long_to_wide(raw, concept_map)
        if (nrow(pivoted) > 0) {
          this_batch_physiology[[nm]] <- pivoted
        }
        # raw is not stored — will be garbage-collected
      } else {
        this_batch_physiology[[nm]] <- as.data.table(raw)
      }
    }

    # --- GCS query ---
    if (!is.null(gcs_sql)) {
      gcs_raw <- dbGetQuery(conn, render_for_batch(gcs_sql))
      if (!is.null(gcs_raw) && nrow(gcs_raw) > 0) {
        this_batch_physiology[["gcs"]] <- as.data.table(gcs_raw)
      }
    }

    batch_physiology[[i]] <- this_batch_physiology

    end_time <- Sys.time()
    message("Batch ", i, " execution time: ",
            round(difftime(end_time, start_time, units = "secs"), 2),
            " seconds")
  }

  # --- Clean up temp table ---
  drop_temp_table(conn, "person_batch", dialect)

  # --- Combine across batches ---
  admissions <- rbindlist(lapply(batch_admissions, as.data.table),
                          use.names = TRUE, fill = TRUE)
  batch_admissions <- NULL  # free memory

  # Combine physiology: for each query name, rbindlist across batches
  all_query_names <- unique(unlist(lapply(batch_physiology, names)))
  physiology_dfs <- list()
  for (nm in all_query_names) {
    parts <- lapply(batch_physiology, function(bp) bp[[nm]])
    parts <- Filter(function(x) !is.null(x) && nrow(x) > 0, parts)
    if (length(parts) > 0) {
      physiology_dfs[[nm]] <- rbindlist(parts, use.names = TRUE, fill = TRUE)
    }
  }
  batch_physiology <- NULL  # free memory

  # --- Merge everything ---
  data <- merge_admissions_and_physiology(admissions, physiology_dfs)
  admissions <- NULL        # free memory
  physiology_dfs <- NULL    # free memory

  # --- Join ground truth columns ---
  if (visit_mode == "ground_truth" && !is.null(resolved_gt)) {
    gt_cols_to_join <- resolved_gt[,
                                   !(colnames(resolved_gt) %in%
                                       c("visit_occurrence_id",
                                         "icu_discharge_datetime")),
                                   drop = FALSE]
    gt_cols_to_join <- gt_cols_to_join[
      !duplicated(gt_cols_to_join[, c("person_id", "icu_admission_datetime")]), ]

    data <- merge(
      data, as.data.table(gt_cols_to_join),
      by = c("person_id", "icu_admission_datetime"),
      all.x = TRUE)
  }

  as_tibble(data)
}
