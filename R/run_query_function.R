# =============================================================================
# run_query_function.R
#
# Main entry point: get_score_variables().
#
# Structure:
#   1. SETUP (once per call): resolve string searches, build all SQL templates,
#      render and translate them. Create ancestor_map if Drug variables exist.
#   2. BATCH LOOP: for each batch of patients, create temp tables, execute
#      pre-built SQL, pivot, collect results.
#   3. MERGE: combine batches, merge physiology with admissions.
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

#' @keywords internal
render_batch_params <- function(sql, visit_mode, person_ids_batch,
                                resolved_gt, dialect) {
  if (visit_mode == "ground_truth") {
    render(sql, ground_truth_values = build_ground_truth_values_clause(
      resolved_gt, person_ids_batch, dialect))
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
create_admission_temp_tables <- function(conn, pasted_visits_rendered, dialect) {
  drop_temp_table(conn, "adm_multi_temp", dialect)
  dbExecute(conn, paste0(
    "CREATE TEMP TABLE adm_multi_temp AS\n",
    pasted_visits_rendered, "\n",
    "SELECT * FROM icu_admission_details_multiple_visits"))
  dbExecute(conn, "ANALYZE adm_multi_temp")
  n_multi <- dbGetQuery(conn, "SELECT COUNT(*) AS n FROM adm_multi_temp")$n
  message("  adm_multi_temp: ", n_multi, " rows")

  drop_temp_table(conn, "adm_details_temp", dialect)
  dbExecute(conn, paste0(
    "CREATE TEMP TABLE adm_details_temp AS\n",
    pasted_visits_rendered, "\n",
    "SELECT * FROM icu_admission_details"))
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
                .(min_val = if (all(is.na(min_val))) NA_real_ else min(min_val, na.rm = TRUE),
                  max_val = if (all(is.na(max_val))) NA_real_ else max(max_val, na.rm = TRUE),
                  unit_name = unit_name[!is.na(unit_name)][1]),
                by = key_cols]
  # Belt-and-braces: catch any Inf that slipped through
  agg[is.infinite(min_val), min_val := NA_real_]
  agg[is.infinite(max_val), max_val := NA_real_]
  if (nrow(agg) == 0) return(data.table())

  id_cols <- c("person_id", "icu_admission_datetime", "time_in_icu")
  formula <- person_id + icu_admission_datetime + time_in_icu ~ short_name

  # Use NA-safe aggregators that return NA (not Inf) for all-NA groups.
  safe_min <- function(x) if (all(is.na(x))) NA_real_ else min(x, na.rm = TRUE)
  safe_max <- function(x) if (all(is.na(x))) NA_real_ else max(x, na.rm = TRUE)

  wide_min <- dcast(agg, formula, value.var = "min_val", fun.aggregate = safe_min, fill = NA)
  wide_max <- dcast(agg, formula, value.var = "max_val", fun.aggregate = safe_max, fill = NA)
  wide_unit <- dcast(agg, formula, value.var = "unit_name",
                     fun.aggregate = function(x) x[!is.na(x)][1], fill = NA)

  # Belt-and-braces: ensure no Inf values survive anywhere in the result.
  # This catches edge cases from dcast aggregation or upstream bugs.
  for (col in setdiff(names(wide_min), id_cols)) {
    if (is.numeric(wide_min[[col]]))
      set(wide_min, which(is.infinite(wide_min[[col]])), col, NA_real_)
  }
  for (col in setdiff(names(wide_max), id_cols)) {
    if (is.numeric(wide_max[[col]]))
      set(wide_max, which(is.infinite(wide_max[[col]])), col, NA_real_)
  }

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
    overlap_cols <- setdiff(names(dt), c(key_cols, new_cols))
    overlap_cols <- setdiff(overlap_cols, names(spine))  # key_cols are expected overlaps
    if (length(overlap_cols) > 0) {
      warning("Physiology table '", nm, "' has column(s) already present from ",
              "an earlier table: ", paste(overlap_cols, collapse = ", "),
              ". Values from the earlier table will be kept.")
    }
    if (length(new_cols) > 0) {
      join_cols <- c(key_cols, new_cols)
      result <- merge(result, dt[, ..join_cols], by = key_cols, all.x = TRUE)
    }
  }

  adm_key <- c("person_id", "icu_admission_datetime")

  # Check for duplicate admissions — same person + same icu_admission_datetime
  # but different visit_detail_id. This would cause a cartesian product in the
  # merge below, silently duplicating all physiology rows.
  dup_adm <- admissions[, .N, by = adm_key][N > 1]
  if (nrow(dup_adm) > 0) {
    warning("DUPLICATE ADMISSIONS DETECTED: ", nrow(dup_adm),
            " (person_id, icu_admission_datetime) combination(s) have multiple rows. ",
            "This will produce a cartesian product — physiology rows will be duplicated. ",
            "First duplicate: person_id=", dup_adm$person_id[1],
            ", icu_admission_datetime=", dup_adm$icu_admission_datetime[1],
            ". Check visit_detail stitching or ground truth for overlapping stays.")
  }

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
# String search resolution
# ---------------------------------------------------------------------------

#' Resolve concept_name/concept_code searches to numeric concept_ids.
#'
#' Queries the concept table once for all string search terms, then:
#'   1. Creates a temp table (string_resolved_ids) with columns
#'      table_name, short_name, concept_id — persists across batches.
#'   2. Removes string search rows from the concepts dataframe (they are
#'      now represented in the temp table).
#'
#' Downstream, build_filtered_temp() adds an OR clause referencing the
#' temp table for tables that have string search variables. The count
#' aggregation uses the same temp table via IN (SELECT ...).
#'
#' @param conn DBI connection.
#' @param concepts The concepts dataframe.
#' @param schema OMOP schema name.
#' @param dialect SQL dialect.
#'
#' @return A list with:
#'   \item{concepts}{Updated concepts dataframe with string search rows removed.}
#'   \item{has_string_ids}{Logical — TRUE if string_resolved_ids temp table was created.}
#'   \item{string_tables}{Character vector of table names that have string search variables.}
#' @keywords internal
resolve_string_searches <- function(conn, concepts, schema, dialect) {

  str_rows <- which(
    concepts$omop_variable %in% c("concept_name", "concept_code") &
      !is.na(concepts$omop_variable))

  if (length(str_rows) == 0) {
    return(list(concepts = concepts, has_string_ids = FALSE,
                string_tables = character(0)))
  }

  str_concepts <- concepts[str_rows, ]
  warning("String searches (concept_name/concept_code) require a full scan of the concept ",
          "table and may be slow (>60s for concept_name on large databases). ",
          "Consider pre-resolving to concept_ids in your concepts file for better performance.",
          call. = FALSE)
  message("  resolving ", length(str_rows), " string search term(s) against concept table...")
  t0 <- Sys.time()

  # Build LIKE expressions for each unique (omop_variable, search_term) pair
  like_exprs <- str_concepts %>%
    distinct(omop_variable, concept_id) %>%
    reframe(expr = glue(
      "LOWER({omop_variable}) LIKE '%{tolower(concept_id)}%'")) %>%
    pull(expr)

  sql <- glue("SELECT DISTINCT concept_id, concept_name, concept_code ",
              "FROM {schema}.concept WHERE {paste(like_exprs, collapse = ' OR ')}")
  sql <- translate(sql, tolower(dialect))
  resolved <- dbGetQuery(conn, sql)

  message("  string search: ", nrow(resolved), " concept_ids resolved (",
          round(difftime(Sys.time(), t0, units = "secs"), 1), "s)")

  if (nrow(resolved) == 0) {
    return(list(concepts = concepts[-str_rows, ], has_string_ids = FALSE,
                string_tables = character(0)))
  }

  # For each string search row, find which resolved concept_ids match
  # that specific search term (not all resolved ids)
  id_rows <- list()
  for (i in seq_len(nrow(str_concepts))) {
    row <- str_concepts[i, ]
    col <- row$omop_variable  # "concept_name" or "concept_code"
    search_term <- tolower(row$concept_id)

    matching_ids <- resolved$concept_id[
      grepl(search_term, tolower(resolved[[col]]), fixed = TRUE)]

    if (length(matching_ids) > 0) {
      id_rows[[length(id_rows) + 1]] <- data.frame(
        table_name = row$table,
        short_name = row$short_name,
        concept_id = as.integer(matching_ids),
        stringsAsFactors = FALSE)
    }
  }

  if (length(id_rows) == 0) {
    return(list(concepts = concepts[-str_rows, ], has_string_ids = FALSE,
                string_tables = character(0)))
  }

  all_ids <- do.call(rbind, id_rows)
  string_tables <- unique(all_ids$table_name)

  # Build table -> unique short_names mapping for count query generation
  string_info <- tapply(all_ids$short_name, all_ids$table_name,
                        function(x) unique(x), simplify = FALSE)
  string_info <- as.list(string_info)

  # Create temp table with resolved IDs
  drop_temp_table(conn, "string_resolved_ids", dialect)
  dbExecute(conn, paste0(
    "CREATE TEMP TABLE string_resolved_ids (",
    "table_name TEXT, short_name TEXT, concept_id INT)"))

  # Insert in batches to avoid huge SQL strings
  batch_sz <- 5000
  for (start in seq(1, nrow(all_ids), by = batch_sz)) {
    end <- min(start + batch_sz - 1, nrow(all_ids))
    chunk <- all_ids[start:end, ]
    vals <- glue_collapse(
      glue("('{chunk$table_name}', '{chunk$short_name}', {chunk$concept_id})"),
      sep = ", ")
    dbExecute(conn, glue("INSERT INTO string_resolved_ids VALUES {vals}"))
  }
  dbExecute(conn, "ANALYZE string_resolved_ids")
  message("  string_resolved_ids: ", nrow(all_ids), " rows across tables: ",
          paste(string_tables, collapse = ", "))

  list(concepts = concepts[-str_rows, ], has_string_ids = TRUE,
       string_tables = string_tables, string_info = string_info)
}


# ---------------------------------------------------------------------------
# Query template building (once per call)
# ---------------------------------------------------------------------------

#' Build all pre-rendered SQL templates for per-table queries.
#'
#' Returns a list of table-level query specs. Each spec contains
#' rendered SQL strings ready for execution — only batch-specific
#' temp table creation varies per batch.
#'
#' @return A named list. Each element has: filtered_stmts (character vector),
#'   long_sql (character or ""), count_sql (character or "").
#' @keywords internal
build_table_query_specs <- function(concepts, variable_names,
                                    window_start_point, cadence,
                                    schema, age_qry, first_window, last_window,
                                    dialect, start_date, end_date,
                                    string_tables = character(0),
                                    string_info = list()) {

  specs <- list()

  tables <- unique(concepts$table[
    concepts$table != "Drug" &
      !(concepts$table == "Visit Detail" & concepts$short_name != "emergency_admission")])

  # Pre-compute date bounds for the filtered temp table date pre-filter.
  # Conservative: floor/ceiling plus ±1 day safety margin ensures no data loss.
  # Exact window filtering still happens in the aggregation step.
  lower_days <- floor(first_window * cadence / 24) - 1L
  upper_days <- ceiling(last_window * cadence / 24) + 1L
  lower_date <- format(as.Date(start_date) + lower_days, "%Y-%m-%d")
  upper_date <- format(as.Date(end_date)   + upper_days, "%Y-%m-%d")

  render_sql <- function(sql) {
    render_and_translate(sql, schema, age_qry, first_window, last_window,
                         dialect, start_date, end_date)
  }

  for (tbl in tables) {
    tbl_concepts <- concepts[
      concepts$table == tbl &
        !(concepts$table == "Visit Detail" & concepts$short_name != "emergency_admission"), ]
    vn <- variable_names[variable_names$table == tbl, ]

    has_strings <- tbl %in% string_tables
    tbl_string_sns <- if (has_strings && tbl %in% names(string_info)) {
      string_info[[tbl]]
    } else character(0)

    # Filtered temp table statements
    raw_stmts <- build_filtered_temp(tbl_concepts, tbl, variable_names,
                                     has_string_ids = has_strings,
                                     lower_date = lower_date,
                                     upper_date = upper_date)
    rendered_stmts <- vapply(raw_stmts, render_sql, character(1))

    # Aggregation queries
    long_raw <- build_long_select(tbl_concepts, tbl, variable_names,
                                  window_start_point, cadence)
    count_raw <- build_count_select(tbl_concepts, tbl, variable_names,
                                    window_start_point, cadence,
                                    has_string_ids = has_strings,
                                    string_short_names = tbl_string_sns)

    specs[[vn$alias]] <- list(
      alias = vn$alias,
      filtered_stmts = rendered_stmts,
      filtered_temp_name = paste0(vn$alias, "_filtered_temp"),
      long_sql = if (long_raw != "") render_sql(long_raw) else "",
      count_sql = if (count_raw != "") render_sql(count_raw) else ""
    )
  }

  specs
}


# ---------------------------------------------------------------------------
# Drug setup (once per call)
# ---------------------------------------------------------------------------

#' Build drug SQL templates and create ancestor_map (once per call).
#'
#' ancestor_map is a session-level temp table that maps descendant_concept_ids
#' to drug group flags. It's created once and persists across batches.
#'
#' @return A list with rendered_filtered_stmts, rendered_select_sql,
#'   temp_tables (names of tables to drop per batch), and
#'   ancestor_map_stmts (to create once).
#' @keywords internal
build_drug_query_spec <- function(concepts, variable_names,
                                  window_start_point, cadence, dialect,
                                  schema, age_qry, first_window, last_window,
                                  start_date, end_date,
                                  has_string_ids = FALSE,
                                  string_short_names = character(0)) {

  drug_result <- build_drug_statements(concepts, variable_names,
                                       window_start_point, cadence, dialect,
                                       has_string_ids = has_string_ids,
                                       string_short_names = string_short_names)
  if (is.null(drug_result)) return(NULL)

  render_sql <- function(sql) {
    render_and_translate(sql, schema, age_qry, first_window, last_window,
                         dialect, start_date, end_date)
  }

  list(
    ancestor_stmts = vapply(drug_result$ancestor_stmts, render_sql, character(1),
                            USE.NAMES = FALSE),
    batch_stmts = vapply(drug_result$batch_stmts, render_sql, character(1),
                         USE.NAMES = FALSE),
    select_sql = render_sql(drug_result$select_sql),
    temp_tables = c("drg_filtered_temp", "drg_tagged")
  )
}


# ---------------------------------------------------------------------------
# Batch execution
# ---------------------------------------------------------------------------

#' Execute one batch using pre-built SQL templates.
#'
#' Only does database work — all SQL strings are pre-rendered.
#' @keywords internal
execute_batch <- function(conn, person_ids_batch,
                          pasted_visits_sql,
                          table_specs, drug_spec, gcs_sql_template,
                          concept_map,
                          schema, age_qry, dialect,
                          start_date, end_date,
                          visit_mode, resolved_gt) {

  # --- 1. Admission temp tables ---
  # Render schema/age/dates and translate BEFORE substituting batch-specific
  # params (VALUES clause or person_ids). This prevents SqlRender from
  # misinterpreting content in the VALUES clause as @parameters.
  pv_rendered <- pasted_visits_sql %>%
    render(schema = schema, age_query = age_qry) %>%
    translate(tolower(dialect)) %>%
    render(start_date = single_quote(start_date),
           end_date = single_quote(end_date))
  pv_rendered <- render_batch_params(
    pv_rendered, visit_mode, person_ids_batch, resolved_gt, dialect)

  create_admission_temp_tables(conn, pv_rendered, dialect)

  # --- 2. Create person_batch from actual admissions ---
  drop_temp_table(conn, "person_batch", dialect)
  dbExecute(conn, paste0(
    "CREATE TEMP TABLE person_batch AS ",
    "SELECT DISTINCT person_id FROM adm_details_temp"))
  dbExecute(conn, "ANALYZE person_batch")
  n_patients <- dbGetQuery(conn, "SELECT COUNT(*) AS n FROM person_batch")$n
  message("  person_batch: ", n_patients, " patients (from ",
          length(person_ids_batch), " candidates)")

  # --- 3. Admissions result ---
  t0 <- Sys.time()
  adm <- as.data.table(dbGetQuery(conn, "SELECT * FROM adm_details_temp"))
  message("  admissions: ", nrow(adm), " rows (",
          round(difftime(Sys.time(), t0, units = "secs"), 1), "s)")

  # --- 4. Per-table physiology queries (pre-rendered SQL) ---
  physiology <- list()
  temp_tables_to_drop <- character(0)

  for (spec in table_specs) {
    # Create filtered temp table
    t0 <- Sys.time()
    for (stmt in spec$filtered_stmts) dbExecute(conn, stmt)
    n_filtered <- dbGetQuery(conn,
                             glue("SELECT COUNT(*) AS n FROM {spec$filtered_temp_name}"))$n
    message("  ", spec$filtered_temp_name, ": ", n_filtered, " rows (",
            round(difftime(Sys.time(), t0, units = "secs"), 1), "s)")
    temp_tables_to_drop <- c(temp_tables_to_drop, spec$filtered_temp_name)

    # Long-format numeric query
    if (spec$long_sql != "") {
      t0 <- Sys.time()
      raw <- dbGetQuery(conn, spec$long_sql)
      elapsed <- round(difftime(Sys.time(), t0, units = "secs"), 1)
      if (!is.null(raw) && nrow(raw) > 0) {
        pivoted <- pivot_long_to_wide(as.data.table(raw), concept_map)
        message("  ", spec$alias, "_long: ", nrow(raw), " -> ",
                nrow(pivoted), " wide rows (", elapsed, "s)")
        if (nrow(pivoted) > 0) physiology[[paste0(spec$alias, "_long")]] <- pivoted
      } else {
        message("  ", spec$alias, "_long: 0 rows (", elapsed, "s)")
      }
    }

    # Count query
    if (spec$count_sql != "") {
      t0 <- Sys.time()
      raw <- dbGetQuery(conn, spec$count_sql)
      elapsed <- round(difftime(Sys.time(), t0, units = "secs"), 1)
      n_rows <- if (is.null(raw)) 0 else nrow(raw)
      message("  ", spec$alias, "_counts: ", n_rows, " rows (", elapsed, "s)")
      if (n_rows > 0) {
        physiology[[paste0(spec$alias, "_counts")]] <- as.data.table(raw)
      } else {
        # Query returned no rows but columns still need to exist in the final
        # result so downstream code doesn't silently skip expected variables.
        # Extract expected count_ column names from the SQL and add them as NA.
        count_cols <- unique(regmatches(spec$count_sql,
                                        gregexpr("count_[a-z0-9_]+", spec$count_sql))[[1]])
        if (length(count_cols) > 0) {
          empty_dt <- data.table(
            person_id = integer(0),
            icu_admission_datetime = character(0),
            time_in_icu = integer(0)
          )
          for (col in count_cols) empty_dt[, (col) := integer(0)]
          physiology[[paste0(spec$alias, "_counts")]] <- empty_dt
        }
      }
    }
  }

  # --- 5. Drug (per-batch statements only — ancestor_map already exists) ---
  if (!is.null(drug_spec)) {
    t0 <- Sys.time()
    for (stmt in drug_spec$batch_stmts) dbExecute(conn, stmt)
    temp_tables_to_drop <- c(temp_tables_to_drop, drug_spec$temp_tables)

    raw <- dbGetQuery(conn, drug_spec$select_sql)
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

  # --- 7. Drop per-batch temp tables ---
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

  # =====================================================================
  # SETUP (once per call)
  # =====================================================================
  setup_start <- Sys.time()
  message("Setting up queries...")

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

  # GCS stored as concept IDs
  gcs_concepts <- concepts[
    concepts$short_name %in% c("gcs_eye", "gcs_motor", "gcs_verbal") &
      concepts$omop_variable == "value_as_concept_id" &
      !is.na(concepts$omop_variable), ]
  concepts <- concepts[
    !(concepts$short_name %in% c("gcs_eye", "gcs_motor", "gcs_verbal") &
        concepts$omop_variable == "value_as_concept_id" &
        !is.na(concepts$omop_variable)), ]

  # --- Resolve string searches (once — queries concept table) ---
  has_string_ids <- FALSE
  string_tables <- character(0)
  string_info <- list()
  if (!dry_run) {
    str_result <- resolve_string_searches(conn, concepts, schema, dialect)
    concepts <- str_result$concepts
    has_string_ids <- str_result$has_string_ids
    string_tables <- str_result$string_tables
    string_info <- if (has_string_ids) str_result$string_info else list()
  }

  # --- Build concept map for R-side pivoting ---
  concept_map <- build_concept_map(concepts)

  # --- Build all SQL templates (once) ---
  table_specs <- build_table_query_specs(
    concepts, variable_names, window_start_point, cadence,
    schema, age_qry, first_window, last_window,
    dialect, start_date, end_date,
    string_tables = string_tables,
    string_info = string_info)

  drug_string_sns <- if (has_string_ids && "Drug" %in% names(string_info)) {
    string_info[["Drug"]]
  } else character(0)

  drug_spec <- build_drug_query_spec(
    concepts, variable_names, window_start_point, cadence, dialect,
    schema, age_qry, first_window, last_window, start_date, end_date,
    has_string_ids = ("Drug" %in% string_tables),
    string_short_names = drug_string_sns)

  # --- Create ancestor_map once (persists across batches) ---
  if (!dry_run && !is.null(drug_spec) && length(drug_spec$ancestor_stmts) > 0) {
    t0 <- Sys.time()
    for (stmt in drug_spec$ancestor_stmts) dbExecute(conn, stmt)
    message("  ancestor_map created (",
            round(difftime(Sys.time(), t0, units = "secs"), 1), "s)")
  }

  # --- GCS template ---
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

  message("Setup completed in ",
          round(difftime(Sys.time(), setup_start, units = "secs"), 1), "s")

  # --- Verbose ---
  if (verbose) {
    message("=== Tables: ", paste(names(table_specs), collapse = ", "))
    if (!is.null(drug_spec)) message("=== Drug query present")
    if (!is.null(gcs_sql_template)) message("=== GCS query present")
  }

  # --- Dry run ---
  if (dry_run) {
    message("Dry run complete.")

    # Build main_query: all SQL in execution order, without patient IDs substituted.
    # Useful for debugging — copy individual statements into a DB client with
    # example patients substituted for @person_ids / @ground_truth_values.
    main_query <- list()
    main_query[["visits"]] <- pasted_visits_sql
    for (spec in table_specs) {
      for (i in seq_along(spec$filtered_stmts))
        main_query[[paste0(spec$alias, "_filtered_", i)]] <- spec$filtered_stmts[[i]]
      if (spec$long_sql  != "") main_query[[paste0(spec$alias, "_long")]]  <- spec$long_sql
      if (spec$count_sql != "") main_query[[paste0(spec$alias, "_count")]] <- spec$count_sql
    }
    if (!is.null(drug_spec)) {
      for (i in seq_along(drug_spec$ancestor_stmts))
        main_query[[paste0("drug_ancestor_", i)]] <- drug_spec$ancestor_stmts[[i]]
      for (i in seq_along(drug_spec$batch_stmts))
        main_query[[paste0("drug_batch_", i)]]    <- drug_spec$batch_stmts[[i]]
      main_query[["drug_select"]] <- drug_spec$select_sql
    }
    if (!is.null(gcs_sql_template)) main_query[["gcs"]] <- gcs_sql_template

    return(list(
      main_query         = main_query,
      pasted_visits_sql  = pasted_visits_sql,
      table_specs        = table_specs,
      drug_spec          = drug_spec,
      gcs_sql_template   = gcs_sql_template,
      concept_map        = concept_map,
      concepts           = concepts))
  }

  # =====================================================================
  # GET PERSON IDS
  # =====================================================================
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

  # =====================================================================
  # BATCH LOOP
  # =====================================================================
  batch_admissions <- vector("list", length(id_batches))
  batch_physiology <- vector("list", length(id_batches))

  for (i in seq_along(id_batches)) {
    message(glue("Running batch {i} of {length(id_batches)}"))
    start_time <- Sys.time()

    result <- execute_batch(
      conn = conn,
      person_ids_batch = id_batches[[i]],
      pasted_visits_sql = pasted_visits_sql,
      table_specs = table_specs,
      drug_spec = drug_spec,
      gcs_sql_template = gcs_sql_template,
      concept_map = concept_map,
      schema = schema,
      age_qry = age_qry,
      dialect = dialect,
      start_date = start_date,
      end_date = end_date,
      visit_mode = visit_mode,
      resolved_gt = resolved_gt)

    batch_admissions[[i]] <- result$admissions
    batch_physiology[[i]] <- result$physiology

    elapsed <- round(difftime(Sys.time(), start_time, units = "secs"), 2)
    message("Batch ", i, " completed in ", elapsed, " seconds")
  }

  # =====================================================================
  # CLEANUP AND MERGE
  # =====================================================================
  drop_temp_table(conn, "person_batch", dialect)
  if (!is.null(drug_spec)) {
    drop_temp_table(conn, "ancestor_map", dialect)
  }
  if (has_string_ids) {
    drop_temp_table(conn, "string_resolved_ids", dialect)
  }

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

  data <- merge_admissions_and_physiology(admissions, physiology_dfs)
  admissions <- NULL
  physiology_dfs <- NULL

  # --- Ground truth columns ---
  if (visit_mode == "ground_truth" && !is.null(resolved_gt)) {
    gt_join <- as.data.table(resolved_gt)
    gt_join <- gt_join[, !names(gt_join) %in%
                         c("visit_occurrence_id", "icu_discharge_datetime"),
                       with = FALSE]
    # resolved_gt has icu_admission_datetime as character (from format_datetime_for_sql),
    # but data has it as POSIXct from Postgres. Cast gt_join to POSIXct for consistency.
    gt_join[, icu_admission_datetime := as.POSIXct(icu_admission_datetime, tz = "UTC")]
    gt_join <- unique(gt_join, by = c("person_id", "icu_admission_datetime"))
    data <- merge(data, gt_join,
                  by = c("person_id", "icu_admission_datetime"), all.x = TRUE)
  }

  total_elapsed <- round(difftime(Sys.time(), setup_start, units = "secs"), 1)
  message("Total time: ", total_elapsed, "s")

  as_tibble(data)
}
