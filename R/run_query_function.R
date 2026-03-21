# =============================================================================
# run_query_function.R
#
# Main entry point: get_score_variables().
#
# Execution model per batch:
#   1. CREATE TEMP TABLE person_batch + ANALYZE
#   2. CREATE TEMP TABLE adm_multi_temp (from pasted visits) + ANALYZE
#   3. CREATE TEMP TABLE adm_details_temp (deduplicated admissions) + ANALYZE
#   4. For each OMOP table:
#      a. CREATE TEMP TABLE {alias}_filtered_temp + ANALYZE
#      b. SELECT (long or count aggregation) referencing temp tables only
#   5. Drug: same pattern with drg_filtered_temp
#   6. GCS query referencing adm_multi_temp
#   7. DROP all temp tables except person_batch (dropped after last batch)
#
# Every query references temp tables — no CTE re-execution.
# =============================================================================


# ---------------------------------------------------------------------------
# Visit SQL
# ---------------------------------------------------------------------------

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
# SQL helpers
# ---------------------------------------------------------------------------

#' Render schema/window params, translate, then render dates.
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

#' Fill batch-specific params (@person_ids or @ground_truth_values).
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

#' @keywords internal
create_person_batch <- function(conn, person_ids_batch, dialect) {
  drop_temp_table(conn, "person_batch", dialect)
  dbExecute(conn, "CREATE TEMP TABLE person_batch (person_id INT)")
  id_rows <- glue_collapse(glue("({person_ids_batch})"), sep = ", ")
  dbExecute(conn, glue("INSERT INTO person_batch VALUES {id_rows}"))
  dbExecute(conn, "ANALYZE person_batch")
  n <- dbGetQuery(conn, "SELECT COUNT(*) AS n FROM person_batch")$n
  message("  person_batch: ", n, " patients loaded")
}

#' Materialise the pasted-visit CTEs as temp tables.
#'
#' Runs the pasted visits SQL once and creates:
#'   - adm_multi_temp (icu_admission_details_multiple_visits)
#'   - adm_details_temp (icu_admission_details — deduplicated with demographics)
#'
#' All subsequent per-table queries reference these temp tables directly.
#'
#' @param conn DBI connection.
#' @param pasted_visits_rendered Fully rendered pasted visits SQL (with
#'   person_ids or ground_truth_values filled in).
#' @param dialect SQL dialect.
#'
#' @keywords internal
create_admission_temp_tables <- function(conn, pasted_visits_rendered, dialect) {

  # adm_multi_temp: the multiple-rows-per-visit table
  drop_temp_table(conn, "adm_multi_temp", dialect)
  sql_multi <- paste0(
    "CREATE TEMP TABLE adm_multi_temp AS\n",
    pasted_visits_rendered, "\n",
    "SELECT * FROM icu_admission_details_multiple_visits")
  dbExecute(conn, sql_multi)
  dbExecute(conn, "ANALYZE adm_multi_temp")
  n_multi <- dbGetQuery(conn, "SELECT COUNT(*) AS n FROM adm_multi_temp")$n
  message("  adm_multi_temp: ", n_multi, " rows")

  # adm_details_temp: deduplicated with demographics
  drop_temp_table(conn, "adm_details_temp", dialect)
  sql_details <- paste0(
    "CREATE TEMP TABLE adm_details_temp AS\n",
    pasted_visits_rendered, "\n",
    "SELECT * FROM icu_admission_details")
  dbExecute(conn, sql_details)
  dbExecute(conn, "ANALYZE adm_details_temp")
  n_details <- dbGetQuery(conn, "SELECT COUNT(*) AS n FROM adm_details_temp")$n
  message("  adm_details_temp: ", n_details, " admissions")
}


# ---------------------------------------------------------------------------
# Long-format pivot
# ---------------------------------------------------------------------------

#' @import data.table
#' @keywords internal
pivot_long_to_wide <- function(long_dt, concept_map) {
  if (nrow(long_dt) == 0) return(data.table())
  long_dt <- as.data.table(long_dt)
  long_dt[, concept_id := as.character(concept_id)]

  mapped <- merge(long_dt, concept_map, by = "concept_id",
                  allow.cartesian = TRUE)
  if (nrow(mapped) == 0) return(data.table())

  # Vectorised filter for additional_filter disambiguation
  has_filter <- !is.na(mapped$filter_col)
  if (any(has_filter)) {
    keep <- rep(TRUE, nrow(mapped))
    for (fc in unique(mapped$filter_col[has_filter])) {
      rows <- which(mapped$filter_col == fc)
      if (fc %in% names(mapped)) {
        keep[rows] <- mapply(
          function(actual, allowed) actual %in% allowed,
          mapped[[fc]][rows], mapped$filter_vals[rows],
          USE.NAMES = FALSE)
      }
    }
    mapped <- mapped[keep]
  }
  if (nrow(mapped) == 0) return(data.table())

  key_cols <- c("person_id", "icu_admission_datetime", "time_in_icu", "short_name")
  agg <- mapped[,
                .(min_val = min(min_val, na.rm = TRUE),
                  max_val = max(max_val, na.rm = TRUE),
                  unit_name = unit_name[!is.na(unit_name)][1]),
                by = key_cols]
  agg[is.infinite(min_val), min_val := NA_real_]
  agg[is.infinite(max_val), max_val := NA_real_]
  if (nrow(agg) == 0) return(data.table())

  id_cols <- c("person_id", "icu_admission_datetime", "time_in_icu")
  formula <- person_id + icu_admission_datetime + time_in_icu ~ short_name

  wide_min <- dcast(agg, formula, value.var = "min_val", fun.aggregate = min, fill = NA)
  wide_max <- dcast(agg, formula, value.var = "max_val", fun.aggregate = max, fill = NA)
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

#' @import data.table
#' @keywords internal
merge_admissions_and_physiology <- function(admissions, physiology_dfs) {
  admissions <- as.data.table(admissions)
  key_cols <- c("person_id", "icu_admission_datetime", "time_in_icu")
  non_empty <- Filter(function(df) !is.null(df) && nrow(df) > 0, physiology_dfs)

  if (length(non_empty) == 0) {
    admissions[, time_in_icu := NA_integer_]
    return(admissions)
  }

  spine <- unique(rbindlist(
    lapply(non_empty, function(df) as.data.table(df)[, ..key_cols]),
    use.names = TRUE))

  result <- spine
  for (nm in names(non_empty)) {
    dt <- as.data.table(non_empty[[nm]])
    new_cols <- setdiff(names(dt), names(result))
    if (length(new_cols) > 0) {
      join_cols <- c(key_cols, new_cols)
      result <- merge(result, dt[, ..join_cols], by = key_cols, all.x = TRUE)
    }
  }

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

#' @keywords internal
normalise_concepts_columns <- function(concepts) {
  if (!"concept_id_value" %in% names(concepts))
    concepts$concept_id_value <- NA
  if (!"name_of_value" %in% names(concepts))
    concepts$name_of_value <- NA
  if (!"additional_filter_variable_name" %in% names(concepts))
    concepts$additional_filter_variable_name <- NA

  has_afvv <- "additional_filter_variable_value" %in% names(concepts)
  has_afv  <- "additional_filter_value" %in% names(concepts)
  if (has_afvv && !has_afv)
    concepts$additional_filter_value <- concepts$additional_filter_variable_value
  else if (has_afv && !has_afvv)
    concepts$additional_filter_variable_value <- concepts$additional_filter_value
  else if (!has_afv && !has_afvv) {
    concepts$additional_filter_value <- NA
    concepts$additional_filter_variable_value <- NA
  }
  concepts
}


# ---------------------------------------------------------------------------
# Batch execution
# ---------------------------------------------------------------------------

#' Execute one batch using temp tables.
#'
#' Per batch:
#'   1. person_batch temp table
#'   2. adm_multi_temp + adm_details_temp (pasted visits, once)
#'   3. Per OMOP table: filtered_temp + SELECT (long or count)
#'   4. Drug: drg_filtered_temp + SELECT
#'   5. GCS
#'   6. Drop per-table temp tables (person_batch survives for next batch)
#'
#' @keywords internal
execute_batch <- function(conn, person_ids_batch,
                          pasted_visits_sql,
                          concepts, variable_names,
                          window_start_point, cadence, dialect,
                          schema, age_qry, first_window, last_window,
                          start_date, end_date,
                          gcs_sql_template,
                          concept_map,
                          visit_mode, resolved_gt) {

  # --- 1. person_batch ---
  create_person_batch(conn, person_ids_batch, dialect)

  # --- 2. Admission temp tables ---
  # Render the pasted visits SQL with batch-specific params, then
  # render remaining @schema/@age_query params
  pv_rendered <- render_batch_params(
    pasted_visits_sql, visit_mode, person_ids_batch, resolved_gt, dialect)
  # The pasted visits SQL still has @schema and @age_query — render those too
  pv_rendered <- pv_rendered %>%
    render(schema = schema, age_query = age_qry) %>%
    translate(tolower(dialect)) %>%
    render(start_date = single_quote(start_date),
           end_date = single_quote(end_date))

  create_admission_temp_tables(conn, pv_rendered, dialect)

  # --- 3. Admissions result ---
  t0 <- Sys.time()
  adm <- as.data.table(dbGetQuery(conn, "SELECT * FROM adm_details_temp"))
  message("  admissions: ", nrow(adm), " rows (",
          round(difftime(Sys.time(), t0, units = "secs"), 1), "s)")

  # --- 4. Per-table physiology queries ---
  physiology <- list()

  physiology_tables <- unique(concepts$table[
    concepts$table != "Drug" &
      !(concepts$table == "Visit Detail" & concepts$short_name != "emergency_admission")])

  temp_tables_to_drop <- character(0)

  for (tbl in physiology_tables) {
    tbl_concepts <- concepts[
      concepts$table == tbl &
        !(concepts$table == "Visit Detail" & concepts$short_name != "emergency_admission"), ]
    vn <- variable_names[variable_names$table == tbl, ]
    filtered_temp_name <- paste0(vn$alias, "_filtered_temp")

    # Create filtered temp table
    t0 <- Sys.time()
    stmts <- build_filtered_temp(tbl_concepts, tbl, variable_names)
    for (stmt in stmts) {
      rendered <- render_and_translate(
        stmt, schema, age_qry, first_window, last_window,
        dialect, start_date, end_date)
      dbExecute(conn, rendered)
    }
    n_filtered <- dbGetQuery(conn,
      glue("SELECT COUNT(*) AS n FROM {filtered_temp_name}"))$n
    message("  ", filtered_temp_name, ": ", n_filtered, " rows (",
            round(difftime(Sys.time(), t0, units = "secs"), 1), "s)")
    temp_tables_to_drop <- c(temp_tables_to_drop, filtered_temp_name)

    # Long-format numeric query
    long_sql <- build_long_select(tbl_concepts, tbl, variable_names,
                                  window_start_point, cadence)
    if (long_sql != "") {
      t0 <- Sys.time()
      rendered <- render_and_translate(
        long_sql, schema, age_qry, first_window, last_window,
        dialect, start_date, end_date)
      raw <- dbGetQuery(conn, rendered)
      elapsed <- round(difftime(Sys.time(), t0, units = "secs"), 1)
      if (!is.null(raw) && nrow(raw) > 0) {
        pivoted <- pivot_long_to_wide(as.data.table(raw), concept_map)
        message("  ", vn$alias, "_long: ", nrow(raw), " -> ",
                nrow(pivoted), " wide rows (", elapsed, "s)")
        if (nrow(pivoted) > 0) physiology[[paste0(vn$alias, "_long")]] <- pivoted
      } else {
        message("  ", vn$alias, "_long: 0 rows (", elapsed, "s)")
      }
    }

    # Count query
    count_sql <- build_count_select(tbl_concepts, tbl, variable_names,
                                    window_start_point, cadence)
    if (count_sql != "") {
      t0 <- Sys.time()
      rendered <- render_and_translate(
        count_sql, schema, age_qry, first_window, last_window,
        dialect, start_date, end_date)
      raw <- dbGetQuery(conn, rendered)
      elapsed <- round(difftime(Sys.time(), t0, units = "secs"), 1)
      n_rows <- if (is.null(raw)) 0 else nrow(raw)
      message("  ", vn$alias, "_counts: ", n_rows, " rows (", elapsed, "s)")
      if (n_rows > 0) physiology[[paste0(vn$alias, "_counts")]] <- as.data.table(raw)
    }
  }

  # --- 5. Drug ---
  drug_result <- build_drug_statements(concepts, variable_names,
                                       window_start_point, cadence, dialect)
  if (!is.null(drug_result)) {
    t0 <- Sys.time()
    for (stmt in drug_result$filtered_stmts) {
      rendered <- render_and_translate(
        stmt, schema, age_qry, first_window, last_window,
        dialect, start_date, end_date)
      dbExecute(conn, rendered)
    }
    temp_tables_to_drop <- c(temp_tables_to_drop, "drg_filtered_temp")

    rendered <- render_and_translate(
      drug_result$select_sql, schema, age_qry, first_window, last_window,
      dialect, start_date, end_date)
    raw <- dbGetQuery(conn, rendered)
    elapsed <- round(difftime(Sys.time(), t0, units = "secs"), 1)
    n_rows <- if (is.null(raw)) 0 else nrow(raw)
    message("  drg: ", n_rows, " rows (", elapsed, "s)")
    if (n_rows > 0) physiology[["drg"]] <- as.data.table(raw)
  }

  # --- 6. GCS ---
  if (!is.null(gcs_sql_template)) {
    t0 <- Sys.time()
    gcs_rendered <- render_batch_params(
      gcs_sql_template, visit_mode, person_ids_batch, resolved_gt, dialect)
    raw <- dbGetQuery(conn, gcs_rendered)
    elapsed <- round(difftime(Sys.time(), t0, units = "secs"), 1)
    n_rows <- if (is.null(raw)) 0 else nrow(raw)
    message("  gcs: ", n_rows, " rows (", elapsed, "s)")
    if (n_rows > 0) physiology[["gcs"]] <- as.data.table(raw)
  }

  # --- 7. Drop per-table temp tables ---
  for (tbl in c(temp_tables_to_drop, "adm_multi_temp", "adm_details_temp")) {
    drop_temp_table(conn, tbl, dialect)
  }

  list(admissions = adm, physiology = physiology)
}


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

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

  # --- Validate ---
  stopifnot(tolower(dialect) %in% c("postgresql", "sql server"))
  stopifnot(visit_mode %in% c("paste", "raw", "ground_truth"))
  if (visit_mode == "paste") stopifnot(is.numeric(paste_gap_hours), paste_gap_hours > 0)
  if (visit_mode == "ground_truth") {
    if (is.null(ground_truth) || !inherits(ground_truth, "gt_config"))
      stop("ground_truth mode requires a gt_config object.")
    validate_ground_truth(ground_truth)
    if (run_validation && !dry_run)
      validate_ground_truth_vs_omop(
        conn = conn, gt_config = ground_truth, schema = schema,
        start_date = start_date, end_date = end_date, dialect = dialect)
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

  # Validate
  dup_filters <- concepts %>%
    group_by(short_name) %>%
    summarise(n = n_distinct(additional_filter_variable_name), .groups = "drop") %>%
    filter(n > 1)
  if (nrow(dup_filters) > 0)
    stop("Multiple additional_filter_variable_name for: ",
         paste(dup_filters$short_name, collapse = ", "))

  # GCS as concept IDs
  gcs_concepts <- concepts[
    concepts$short_name %in% c("gcs_eye", "gcs_motor", "gcs_verbal") &
      concepts$omop_variable == "value_as_concept_id" &
      !is.na(concepts$omop_variable), ]
  concepts <- concepts[
    !(concepts$short_name %in% c("gcs_eye", "gcs_motor", "gcs_verbal") &
        concepts$omop_variable == "value_as_concept_id" &
        !is.na(concepts$omop_variable)), ]

  concept_map <- build_concept_map(concepts)

  # GCS query template — still uses CTE preamble since it has its own
  # measurement logic, but now references person_batch for filtering.
  gcs_sql_template <- if (nrow(gcs_concepts) > 0) {
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
    message("=== Pasted Visits SQL ===\n", pasted_visits_sql)
    message("=== Tables: ", paste(unique(concepts$table), collapse = ", "))
    if (!is.null(gcs_sql_template))
      message("=== GCS Query ===\n", gcs_sql_template)
  }

  # --- Dry run ---
  if (dry_run) {
    message("Dry run complete.")
    return(list(
      pasted_visits_sql = pasted_visits_sql,
      gcs_sql_template = gcs_sql_template,
      concept_map = concept_map,
      concepts = concepts,
      variable_names = variable_names))
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
      conn = conn,
      person_ids_batch = id_batches[[i]],
      pasted_visits_sql = pasted_visits_sql,
      concepts = concepts,
      variable_names = variable_names,
      window_start_point = window_start_point,
      cadence = cadence,
      dialect = dialect,
      schema = schema,
      age_qry = age_qry,
      first_window = first_window,
      last_window = last_window,
      start_date = start_date,
      end_date = end_date,
      gcs_sql_template = gcs_sql_template,
      concept_map = concept_map,
      visit_mode = visit_mode,
      resolved_gt = resolved_gt)

    batch_admissions[[i]] <- result$admissions
    batch_physiology[[i]] <- result$physiology

    elapsed <- round(difftime(Sys.time(), start_time, units = "secs"), 2)
    message("Batch ", i, " completed in ", elapsed, " seconds")
  }

  # --- Clean up ---
  drop_temp_table(conn, "person_batch", dialect)

  # --- Combine batches ---
  admissions <- rbindlist(batch_admissions, use.names = TRUE, fill = TRUE)
  batch_admissions <- NULL

  all_query_names <- unique(unlist(lapply(batch_physiology, names)))
  physiology_dfs <- list()
  for (nm in all_query_names) {
    parts <- Filter(
      function(x) !is.null(x) && nrow(x) > 0,
      lapply(batch_physiology, function(bp) bp[[nm]]))
    if (length(parts) > 0)
      physiology_dfs[[nm]] <- rbindlist(parts, use.names = TRUE, fill = TRUE)
  }
  batch_physiology <- NULL

  # --- Merge ---
  data <- merge_admissions_and_physiology(admissions, physiology_dfs)
  admissions <- NULL
  physiology_dfs <- NULL

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
