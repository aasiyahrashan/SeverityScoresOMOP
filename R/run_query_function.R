# =============================================================================
# run_query_function.R
#
# Main entry point: get_score_variables(). Orchestrates query construction,
# batched execution, long-format pivoting, and result merging.
#
# Helper functions are at module level (not nested inside the main function)
# for testability and clarity.
# =============================================================================


# ---------------------------------------------------------------------------
# Visit SQL builder
# ---------------------------------------------------------------------------

#' Build the pasted_visits SQL string based on visit_mode.
#'
#' @param visit_mode One of "paste", "raw", "ground_truth".
#' @param dialect SQL dialect.
#' @param paste_gap_hours Gap threshold for paste mode.
#' @param ground_truth A gt_config object (ground_truth mode only).
#'
#' @return A character string with the rendered visit identification SQL.
#' @importFrom readr read_file
#' @importFrom SqlRender render
#' @importFrom glue glue
#' @keywords internal
build_visit_sql <- function(visit_mode, dialect,
                            paste_gap_hours = 6,
                            ground_truth = NULL) {

  if (visit_mode == "ground_truth") {
    gt_hosp_adm <- if (!is.null(ground_truth$gt_hospital_admission_col)) {
      glue("gt.{ground_truth$gt_hospital_admission_col}")
    } else "NULL"
    gt_hosp_dis <- if (!is.null(ground_truth$gt_hospital_discharge_col)) {
      glue("gt.{ground_truth$gt_hospital_discharge_col}")
    } else "NULL"

    read_file(system.file("ground_truth_admission_details.sql",
                          package = "SeverityScoresOMOP")) %>%
      render(gt_hospital_admission_expr = gt_hosp_adm,
             gt_hospital_discharge_expr = gt_hosp_dis)

  } else if (visit_mode == "raw") {
    read_file(system.file("raw_visit_details.sql",
                          package = "SeverityScoresOMOP"))
  } else {
    read_file(system.file("paste_disjoint_icu_visits.sql",
                          package = "SeverityScoresOMOP")) %>%
      render(paste_gap_hours = paste_gap_hours)
  }
}


# ---------------------------------------------------------------------------
# SQL rendering helpers
# ---------------------------------------------------------------------------

#' Render and translate a SQL template with standard parameters.
#'
#' Applies schema, age_query, and window parameters, translates to dialect,
#' then renders date parameters (must happen after translate due to SqlRender
#' bug with date literals).
#'
#' @param sql SQL string with \@ placeholders.
#' @param schema OMOP schema name.
#' @param age_qry Rendered age expression.
#' @param first_window First time window.
#' @param last_window Last time window.
#' @param dialect SQL dialect string.
#' @param start_date Start date as character.
#' @param end_date End date as character.
#'
#' @return Fully rendered SQL string.
#' @importFrom SqlRender render translate
#' @importFrom glue single_quote
#' @keywords internal
render_and_translate <- function(sql, schema, age_qry,
                                 first_window, last_window,
                                 dialect, start_date, end_date) {
  sql %>%
    render(schema = schema, age_query = age_qry,
           first_window = first_window, last_window = last_window) %>%
    translate(tolower(dialect)) %>%
    render(start_date = single_quote(start_date),
           end_date = single_quote(end_date))
}


#' Render batch-specific parameters into a SQL template.
#'
#' In ground truth mode, fills \@ground_truth_values.
#' In paste/raw mode, fills \@person_ids.
#'
#' @param sql SQL template string.
#' @param visit_mode Visit mode string.
#' @param person_ids_batch Integer vector of person IDs for this batch.
#' @param resolved_gt Resolved ground truth dataframe (or NULL).
#' @param dialect SQL dialect.
#'
#' @return Rendered SQL string.
#' @importFrom SqlRender render
#' @importFrom glue glue_collapse
#' @keywords internal
render_batch_params <- function(sql, visit_mode, person_ids_batch,
                                resolved_gt, dialect) {
  if (visit_mode == "ground_truth") {
    gt_values <- build_ground_truth_values_clause(
      resolved_gt, person_ids_batch, dialect)
    render(sql, ground_truth_values = gt_values)
  } else {
    render(sql, person_ids = glue_collapse(person_ids_batch, sep = ", "))
  }
}


# ---------------------------------------------------------------------------
# Temp table management
# ---------------------------------------------------------------------------

#' Drop a temp table (dialect-aware).
#'
#' Uses DROP TABLE IF EXISTS on PostgreSQL (no error if missing).
#' Uses conditional drop on SQL Server.
#'
#' @param conn DBI connection.
#' @param table_name Name of the temp table.
#' @param dialect SQL dialect.
#' @keywords internal
drop_temp_table <- function(conn, table_name, dialect) {
  sql <- if (tolower(dialect) == "sql server") {
    glue("IF OBJECT_ID('tempdb..#{table_name}') IS NOT NULL ",
         "DROP TABLE #{table_name}")
  } else {
    glue("DROP TABLE IF EXISTS {table_name}")
  }
  dbExecute(conn, sql)
}


#' Create and populate the person_batch temp table.
#'
#' @param conn DBI connection.
#' @param person_ids_batch Integer vector of person IDs.
#' @param dialect SQL dialect.
#'
#' @importFrom DBI dbExecute
#' @importFrom glue glue glue_collapse
#' @keywords internal
create_person_batch <- function(conn, person_ids_batch, dialect) {
  drop_temp_table(conn, "person_batch", dialect)

  # Use raw SQL — CREATE TEMP TABLE is standard and doesn't need SqlRender.
  # On SQL Server, TEMP TABLE syntax differs but SqlRender's translation
  # of this specific statement has been unreliable in some versions.
  if (tolower(dialect) == "sql server") {
    dbExecute(conn, "CREATE TABLE #person_batch (person_id INT)")
  } else {
    dbExecute(conn, "CREATE TEMP TABLE person_batch (person_id INT)")
  }

  id_rows <- glue_collapse(glue("({person_ids_batch})"), sep = ", ")
  dbExecute(conn, glue("INSERT INTO person_batch VALUES {id_rows}"))

  if (tolower(dialect) == "sql server") {
    dbExecute(conn, "UPDATE STATISTICS #person_batch")
  } else {
    dbExecute(conn, "ANALYZE person_batch")
  }

  # Verify the table was populated
  n <- dbGetQuery(conn, "SELECT COUNT(*) AS n FROM person_batch")$n
  message("  person_batch: ", n, " patients loaded")
}


# ---------------------------------------------------------------------------
# Long-format pivot
# ---------------------------------------------------------------------------

#' Pivot long-format query results to wide format.
#'
#' Converts rows keyed by concept_id into min_{short_name}, max_{short_name},
#' unit_{short_name} columns using the concept map.
#'
#' When additional filter columns are present (e.g. measurement_source_value),
#' only rows matching the filter values for each short_name are included.
#' This disambiguation is vectorised — no row-by-row loop.
#'
#' @param long_dt data.table from a long-format query.
#' @param concept_map data.table from build_concept_map().
#'
#' @return A wide-format data.table, or an empty data.table.
#' @import data.table
#' @keywords internal
pivot_long_to_wide <- function(long_dt, concept_map) {

  if (nrow(long_dt) == 0) return(data.table())
  long_dt <- as.data.table(long_dt)
  long_dt[, concept_id := as.character(concept_id)]

  # Expand: concept_id can map to multiple short_names (via filters)
  mapped <- merge(long_dt, concept_map, by = "concept_id",
                  allow.cartesian = TRUE)
  if (nrow(mapped) == 0) return(data.table())

  # Vectorised filter: keep rows where (a) no filter is defined, or
  # (b) the filter column value is in the allowed set.
  has_filter <- !is.na(mapped$filter_col)
  if (any(has_filter)) {
    # For each unique filter_col, check all rows with that filter at once
    keep <- rep(TRUE, nrow(mapped))
    for (fc in unique(mapped$filter_col[has_filter])) {
      rows_with_fc <- which(mapped$filter_col == fc)
      if (fc %in% names(mapped)) {
        actual_vals <- mapped[[fc]][rows_with_fc]
        allowed_vals <- mapped$filter_vals[rows_with_fc]
        keep[rows_with_fc] <- mapply(
          function(actual, allowed) actual %in% allowed,
          actual_vals, allowed_vals,
          USE.NAMES = FALSE)
      }
    }
    mapped <- mapped[keep]
  }

  if (nrow(mapped) == 0) return(data.table())

  # Aggregate: min of mins, max of maxes, first non-NA unit
  key_cols <- c("person_id", "icu_admission_datetime", "time_in_icu", "short_name")
  agg <- mapped[,
                .(min_val = min(min_val, na.rm = TRUE),
                  max_val = max(max_val, na.rm = TRUE),
                  unit_name = unit_name[!is.na(unit_name)][1]),
                by = key_cols]
  agg[is.infinite(min_val), min_val := NA_real_]
  agg[is.infinite(max_val), max_val := NA_real_]
  if (nrow(agg) == 0) return(data.table())

  # Pivot to wide: three dcast calls
  id_cols <- c("person_id", "icu_admission_datetime", "time_in_icu")
  formula <- person_id + icu_admission_datetime + time_in_icu ~ short_name

  wide_min <- dcast(agg, formula, value.var = "min_val",
                    fun.aggregate = min, fill = NA)
  wide_max <- dcast(agg, formula, value.var = "max_val",
                    fun.aggregate = max, fill = NA)
  wide_unit <- dcast(agg, formula, value.var = "unit_name",
                     fun.aggregate = function(x) x[!is.na(x)][1], fill = NA)

  short_names <- setdiff(names(wide_min), id_cols)
  setnames(wide_min, short_names, paste0("min_", short_names))
  setnames(wide_max, short_names, paste0("max_", short_names))
  setnames(wide_unit, short_names, paste0("unit_", short_names))

  result <- merge(wide_min, wide_max, by = id_cols)
  merge(result, wide_unit, by = id_cols)
}


# ---------------------------------------------------------------------------
# Result merging
# ---------------------------------------------------------------------------

#' Merge physiology dataframes onto an admission spine.
#'
#' Builds a complete time_in_icu grid from all physiology results, then
#' merges everything together and RIGHT JOINs to admissions.
#'
#' @param admissions data.table of admission details (one row per ICU stay).
#' @param physiology_dfs Named list of data.tables, each with
#'   person_id, icu_admission_datetime, time_in_icu as key columns.
#'
#' @return A data.table with one row per (person, stay, time_in_icu).
#' @import data.table
#' @keywords internal
merge_admissions_and_physiology <- function(admissions, physiology_dfs) {

  admissions <- as.data.table(admissions)
  key_cols <- c("person_id", "icu_admission_datetime", "time_in_icu")
  non_empty <- Filter(function(df) !is.null(df) && nrow(df) > 0, physiology_dfs)

  if (length(non_empty) == 0) {
    # No physiology data at all — return admissions with no time rows.
    admissions[, time_in_icu := NA_integer_]
    return(admissions)
  }

  # Build spine: unique (person, stay, window) from all physiology data
  spine <- unique(rbindlist(
    lapply(non_empty, function(df) as.data.table(df)[, ..key_cols]),
    use.names = TRUE))

  # Merge each physiology df onto the spine
  result <- spine
  for (nm in names(non_empty)) {
    dt <- as.data.table(non_empty[[nm]])
    new_cols <- setdiff(names(dt), names(result))
    if (length(new_cols) > 0) {
      join_cols <- c(key_cols, new_cols)
      result <- merge(result, dt[, ..join_cols], by = key_cols, all.x = TRUE)
    }
  }

  # RIGHT JOIN to admissions, filter to rows with physiology data
  adm_key <- c("person_id", "icu_admission_datetime")
  result <- merge(admissions, result, by = adm_key,
                  all.x = TRUE, allow.cartesian = TRUE)
  result <- result[!is.na(time_in_icu)]
  setorder(result, person_id, icu_admission_datetime, time_in_icu)
  result
}


# ---------------------------------------------------------------------------
# Concepts normalisation
# ---------------------------------------------------------------------------

#' Normalise concepts dataframe columns for backward compatibility.
#'
#' Different concepts CSV files use different column names. This ensures
#' all required columns exist with consistent names.
#'
#' @param concepts A dataframe from reading a concepts CSV.
#' @return The normalised dataframe.
#' @keywords internal
normalise_concepts_columns <- function(concepts) {

  if (!"concept_id_value" %in% names(concepts))
    concepts$concept_id_value <- NA
  if (!"name_of_value" %in% names(concepts))
    concepts$name_of_value <- NA
  if (!"additional_filter_variable_name" %in% names(concepts))
    concepts$additional_filter_variable_name <- NA

  # Some files use additional_filter_variable_value, others additional_filter_value
  has_afvv <- "additional_filter_variable_value" %in% names(concepts)
  has_afv  <- "additional_filter_value" %in% names(concepts)

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


# ---------------------------------------------------------------------------
# Query template construction
# ---------------------------------------------------------------------------

#' Build all SQL query templates for a set of concepts.
#'
#' Creates one template per table (long and/or count), plus drug and GCS
#' if applicable. Each template is a self-contained SQL string with
#' \@person_ids or \@ground_truth_values as the only remaining placeholder.
#'
#' @param concepts Normalised concepts dataframe.
#' @param variable_names Variable names dataframe.
#' @param pasted_visits_sql Pre-rendered visit SQL.
#' @param window_start_point Windowing mode.
#' @param cadence Cadence in hours.
#' @param dialect SQL dialect.
#' @param schema OMOP schema.
#' @param age_qry Rendered age expression.
#' @param first_window First window.
#' @param last_window Last window.
#' @param start_date Start date.
#' @param end_date End date.
#'
#' @return A named list of lists, each with \code{sql} and \code{type}
#'   ("long" or "counts").
#' @keywords internal
build_query_templates <- function(concepts, variable_names, pasted_visits_sql,
                                  window_start_point, cadence, dialect,
                                  schema, age_qry, first_window, last_window,
                                  start_date, end_date) {

  templates <- list()

  # Non-drug, non-visit-detail-except-emergency tables
  physiology_tables <- unique(concepts$table[
    concepts$table != "Drug" &
      !(concepts$table == "Visit Detail" & concepts$short_name != "emergency_admission")])

  for (tbl in physiology_tables) {
    tbl_concepts <- concepts[
      concepts$table == tbl &
        !(concepts$table == "Visit Detail" & concepts$short_name != "emergency_admission"), ]
    vn <- variable_names[variable_names$table == tbl, ]

    long_sql <- build_long_query(tbl_concepts, tbl, variable_names,
                                 pasted_visits_sql, window_start_point, cadence)
    if (long_sql != "") {
      templates[[paste0(vn$alias, "_long")]] <- list(
        sql = render_and_translate(long_sql, schema, age_qry, first_window,
                                   last_window, dialect, start_date, end_date),
        type = "long")
    }

    count_sql <- build_count_query(tbl_concepts, tbl, variable_names,
                                   pasted_visits_sql, window_start_point, cadence)
    if (count_sql != "") {
      templates[[paste0(vn$alias, "_counts")]] <- list(
        sql = render_and_translate(count_sql, schema, age_qry, first_window,
                                   last_window, dialect, start_date, end_date),
        type = "counts")
    }
  }

  # Drug
  drug_sql <- build_drug_query(concepts, variable_names, pasted_visits_sql,
                               window_start_point, cadence, dialect)
  if (drug_sql != "") {
    templates[["drg"]] <- list(
      sql = render_and_translate(drug_sql, schema, age_qry, first_window,
                                 last_window, dialect, start_date, end_date),
      type = "counts")
  }

  templates
}


# ---------------------------------------------------------------------------
# Batch execution
# ---------------------------------------------------------------------------

#' Execute one batch: fetch admission + physiology + GCS data.
#'
#' Creates the person_batch temp table, runs all queries, pivots long-format
#' results immediately, and returns processed data. Raw long-format rows
#' are discarded after pivoting.
#'
#' @param conn DBI connection.
#' @param person_ids_batch Integer vector of person IDs.
#' @param admission_sql Admission query template.
#' @param query_templates Named list from build_query_templates().
#' @param gcs_sql GCS query template (or NULL).
#' @param concept_map data.table from build_concept_map().
#' @param visit_mode Visit mode.
#' @param resolved_gt Resolved ground truth df (or NULL).
#' @param dialect SQL dialect.
#'
#' @return A list with \code{admissions} (data.table) and
#'   \code{physiology} (named list of data.tables).
#'
#' @importFrom DBI dbGetQuery dbExecute
#' @keywords internal
execute_batch <- function(conn, person_ids_batch, admission_sql,
                          query_templates, gcs_sql, concept_map,
                          visit_mode, resolved_gt, dialect) {

  create_person_batch(conn, person_ids_batch, dialect)

  render_batch <- function(sql) {
    render_batch_params(sql, visit_mode, person_ids_batch, resolved_gt, dialect)
  }

  # Admission details
  t0 <- Sys.time()
  adm <- as.data.table(dbGetQuery(conn, render_batch(admission_sql)))
  message("  admissions: ", nrow(adm), " rows (",
          round(difftime(Sys.time(), t0, units = "secs"), 1), "s)")

  # Physiology queries — pivot long-format immediately
  physiology <- list()
  for (nm in names(query_templates)) {
    t0 <- Sys.time()
    raw <- dbGetQuery(conn, render_batch(query_templates[[nm]]$sql))
    elapsed <- round(difftime(Sys.time(), t0, units = "secs"), 1)

    if (is.null(raw) || nrow(raw) == 0) {
      message("  ", nm, ": 0 rows (", elapsed, "s)")
      next
    }

    if (query_templates[[nm]]$type == "long") {
      pivoted <- pivot_long_to_wide(as.data.table(raw), concept_map)
      message("  ", nm, ": ", nrow(raw), " long rows -> ",
              nrow(pivoted), " wide rows (", elapsed, "s)")
      if (nrow(pivoted) > 0) physiology[[nm]] <- pivoted
    } else {
      message("  ", nm, ": ", nrow(raw), " rows (", elapsed, "s)")
      physiology[[nm]] <- as.data.table(raw)
    }
  }

  # GCS
  if (!is.null(gcs_sql)) {
    t0 <- Sys.time()
    gcs_raw <- dbGetQuery(conn, render_batch(gcs_sql))
    message("  gcs: ", if (is.null(gcs_raw)) 0 else nrow(gcs_raw),
            " rows (", round(difftime(Sys.time(), t0, units = "secs"), 1), "s)")
    if (!is.null(gcs_raw) && nrow(gcs_raw) > 0) {
      physiology[["gcs"]] <- as.data.table(gcs_raw)
    }
  }

  list(admissions = adm, physiology = physiology)
}


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

#' Extract physiology variables from an OMOP CDM database.
#'
#' Queries clinical tables for variables defined in a concepts CSV file,
#' using optimised per-table queries with long-format SQL aggregation for
#' numeric variables and CASE WHEN for count variables. Results are merged
#' in R.
#'
#' @param conn DBI connection to an OMOP database.
#' @param dialect SQL dialect: "postgresql" or "sql server".
#' @param schema OMOP schema name.
#' @param start_date Earliest ICU admission date (inclusive, character).
#' @param end_date Latest ICU admission date (inclusive, character).
#' @param first_window First time window (inclusive).
#' @param last_window Last time window (inclusive).
#' @param concepts_file_path Path to the concepts CSV file.
#' @param severity_score Character vector of score names to filter by.
#' @param age_method "dob" or "year_only". Default "dob".
#' @param cadence Window size in hours. Default 24.
#' @param window_start_point "calendar_date" or "icu_admission_time".
#' @param batch_size Patients per batch. Default 10000.
#' @param visit_mode "paste", "raw", or "ground_truth". Default "paste".
#' @param paste_gap_hours Max gap for paste mode. Default 6.
#' @param ground_truth A gt_config object for ground_truth mode.
#' @param run_validation Run ground truth validation checks. Default TRUE.
#' @param verbose Print rendered SQL. Default FALSE.
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
#' @importFrom tibble as_tibble
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

  # --- Argument validation ---
  stopifnot(tolower(dialect) %in% c("postgresql", "sql server"))
  stopifnot(visit_mode %in% c("paste", "raw", "ground_truth"))

  if (visit_mode == "paste") {
    stopifnot(is.numeric(paste_gap_hours), paste_gap_hours > 0)
  }
  if (visit_mode == "ground_truth") {
    if (is.null(ground_truth) || !inherits(ground_truth, "gt_config")) {
      stop("ground_truth mode requires a gt_config object. ",
           "Create one with ground_truth_config().")
    }
    validate_ground_truth(ground_truth)
    if (run_validation && !dry_run) {
      message("Validating ground truth against OMOP...")
      validate_ground_truth_vs_omop(
        conn = conn, gt_config = ground_truth, schema = schema,
        start_date = start_date, end_date = end_date, dialect = dialect)
    }
  }

  # --- Shared components ---
  age_qry <- age_query(age_method)
  pasted_visits_sql <- build_visit_sql(visit_mode, dialect,
                                       paste_gap_hours, ground_truth)

  variable_names <- read_delim(
    system.file("variable_names.csv", package = "SeverityScoresOMOP"),
    show_col_types = FALSE)

  concepts <- read_delim(concepts_file_path, show_col_types = FALSE) %>%
    filter(str_detect(score, paste(severity_score, collapse = "|"))) %>%
    filter(table %in% c("Measurement", "Observation", "Condition",
                        "Procedure", "Visit Detail", "Device", "Drug")) %>%
    mutate(concept_id = as.character(concept_id)) %>%
    normalise_concepts_columns()

  # Validate concepts
  dup_filters <- concepts %>%
    group_by(short_name) %>%
    summarise(n = n_distinct(additional_filter_variable_name), .groups = "drop") %>%
    filter(n > 1)
  if (nrow(dup_filters) > 0) {
    stop("Multiple additional_filter_variable_name values for short_name(s): ",
         paste(dup_filters$short_name, collapse = ", "))
  }

  string_with_val <- concepts %>%
    filter(omop_variable %in% c("concept_name", "concept_code"),
           !is.na(concept_id_value))
  if (nrow(string_with_val) > 0) {
    stop("concept_name/concept_code rows must not have concept_id_value set. ",
         "Affected: ", paste(string_with_val$short_name, collapse = ", "))
  }

  # GCS as concept IDs needs a separate query
  gcs_concepts <- concepts[
    concepts$short_name %in% c("gcs_eye", "gcs_motor", "gcs_verbal") &
      concepts$omop_variable == "value_as_concept_id" &
      !is.na(concepts$omop_variable), ]
  concepts <- concepts[
    !(concepts$short_name %in% c("gcs_eye", "gcs_motor", "gcs_verbal") &
        concepts$omop_variable == "value_as_concept_id" &
        !is.na(concepts$omop_variable)), ]

  concept_map <- build_concept_map(concepts)

  # --- Build query templates ---
  admission_sql <- read_file(
    system.file("physiology_variables.sql",
                package = "SeverityScoresOMOP")) %>%
    render(pasted_visits = pasted_visits_sql,
           schema = schema, age_query = age_qry) %>%
    translate(tolower(dialect)) %>%
    render(start_date = single_quote(start_date),
           end_date = single_quote(end_date))

  query_templates <- build_query_templates(
    concepts, variable_names, pasted_visits_sql, window_start_point, cadence,
    dialect, schema, age_qry, first_window, last_window, start_date, end_date)

  gcs_sql <- if (nrow(gcs_concepts) > 0) {
    read_file(system.file("gcs_if_stored_as_concept.sql",
                          package = "SeverityScoresOMOP")) %>%
      render(pasted_visits = pasted_visits_sql, schema = schema,
             first_window = first_window, last_window = last_window,
             window_measurement = window_query(
               window_start_point, "measurement_datetime",
               "measurement_date", cadence)) %>%
      translate(tolower(dialect)) %>%
      render(start_date = single_quote(start_date),
             end_date = single_quote(end_date))
  } else NULL

  # --- Verbose ---
  if (verbose) {
    message("=== Admission Query ===\n", admission_sql)
    for (nm in names(query_templates))
      message("=== ", nm, " Query ===\n", query_templates[[nm]]$sql)
    if (!is.null(gcs_sql))
      message("=== GCS Query ===\n", gcs_sql)
  }

  # --- Dry run ---
  if (dry_run) {
    render_example <- function(sql) {
      if (visit_mode == "ground_truth") {
        ts <- if (tolower(dialect) == "postgresql") "TIMESTAMP" else "DATETIME"
        render(sql, ground_truth_values = glue(
          "(1, 100, CAST('2000-01-01 00:00:00' AS {ts}), ",
          "CAST('2000-01-02 00:00:00' AS {ts}))"))
      } else render(sql, person_ids = "1, 2, 3")
    }
    message("Dry run complete.")
    return(list(
      admission_sql = render_example(admission_sql),
      query_templates = lapply(query_templates, function(q)
        list(sql = render_example(q$sql), type = q$type)),
      gcs_sql = if (!is.null(gcs_sql)) render_example(gcs_sql),
      concept_map = concept_map))
  }

  # --- Get person IDs ---
  resolved_gt <- NULL
  if (visit_mode == "ground_truth") {
    resolved_gt <- resolve_ground_truth_ids(
      conn = conn, gt_config = ground_truth, dialect = dialect)
    person_ids <- unique(resolved_gt$person_id)
  } else {
    person_ids <- dbGetQuery(conn, glue(
      "SELECT DISTINCT person_id FROM {schema}.visit_detail ",
      "WHERE visit_detail_concept_id IN (581379, 32037) ",
      "AND COALESCE(visit_detail_start_datetime, ",
      "visit_detail_start_date) <= '{end_date}'"))$person_id
  }

  id_batches <- split(person_ids, ceiling(seq_along(person_ids) / batch_size))

  # --- Execute batches ---
  batch_admissions <- vector("list", length(id_batches))
  batch_physiology <- vector("list", length(id_batches))

  for (i in seq_along(id_batches)) {
    message(glue("Running batch {i} of {length(id_batches)}"))
    start_time <- Sys.time()

    result <- execute_batch(
      conn, id_batches[[i]], admission_sql, query_templates, gcs_sql,
      concept_map, visit_mode, resolved_gt, dialect)

    batch_admissions[[i]] <- result$admissions
    batch_physiology[[i]] <- result$physiology

    elapsed <- round(difftime(Sys.time(), start_time, units = "secs"), 2)
    message("Batch ", i, " completed in ", elapsed, " seconds")
  }

  # --- Clean up temp table ---
  drop_temp_table(conn, "person_batch", dialect)

  # --- Combine batches ---
  admissions <- rbindlist(batch_admissions, use.names = TRUE, fill = TRUE)
  batch_admissions <- NULL  # free memory

  all_query_names <- unique(unlist(lapply(batch_physiology, names)))
  physiology_dfs <- list()
  for (nm in all_query_names) {
    parts <- Filter(
      function(x) !is.null(x) && nrow(x) > 0,
      lapply(batch_physiology, function(bp) bp[[nm]]))
    if (length(parts) > 0)
      physiology_dfs[[nm]] <- rbindlist(parts, use.names = TRUE, fill = TRUE)
  }
  batch_physiology <- NULL  # free memory

  # --- Merge ---
  data <- merge_admissions_and_physiology(admissions, physiology_dfs)
  admissions <- NULL       # free memory
  physiology_dfs <- NULL   # free memory

  # --- Ground truth columns ---
  if (visit_mode == "ground_truth" && !is.null(resolved_gt)) {
    gt_join <- as.data.table(resolved_gt)
    gt_join <- gt_join[, !names(gt_join) %in%
                          c("visit_occurrence_id", "icu_discharge_datetime"),
                        with = FALSE]
    gt_join <- unique(gt_join, by = c("person_id", "icu_admission_datetime"))
    data <- merge(data, gt_join,
                  by = c("person_id", "icu_admission_datetime"), all.x = TRUE)
  }

  as_tibble(data)
}
