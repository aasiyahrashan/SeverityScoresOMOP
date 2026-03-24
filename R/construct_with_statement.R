# =============================================================================
# construct_with_statement.R
#
# Builds SQL for per-table physiology extraction. All queries reference
# pre-materialised temp tables (person_batch, adm_multi_temp) — no CTEs
# for visit pasting.
#
# Key design: the _filtered temp table does NOT join the concept table.
# It only filters by concept_id IN (...) and person_batch. The concept
# table join (for unit names or string searches) happens in the aggregation
# step on the much smaller filtered dataset.
# =============================================================================


#' Build statements to create a filtered temp table for one OMOP table.
#'
#' Filters the clinical table by person_batch (IN subquery) and concept IDs.
#' Pre-joins unit names (LEFT JOIN concept on unit_concept_id) for tables
#' with numeric variables.
#'
#' When multiple filter types exist (direct IDs, ancestor subquery,
#' string_resolved_ids), uses UNION instead of OR. This prevents
#' Postgres from falling back to sequential scans when it can't optimise
#' an OR across different filter strategies (e.g. IN-list vs subquery).
#'
#' @param table_concepts Concepts dataframe, pre-filtered to this table.
#' @param table_name OMOP table name.
#' @param variable_names Variable names dataframe.
#' @param has_string_ids Logical. If TRUE, string_resolved_ids temp table
#'   exists and this table has entries in it.
#'
#' @return Character vector: DROP, CREATE TEMP TABLE, ANALYZE statements.
#' @importFrom glue glue glue_collapse
#' @keywords internal
build_filtered_temp <- function(table_concepts, table_name, variable_names,
                                has_string_ids = FALSE,
                                lower_date = NULL, upper_date = NULL) {

  vn <- variable_names[variable_names$table == table_name, ]
  temp_name <- paste0(vn$alias, "_filtered_temp")

  # Visit Detail has no datetime columns
  has_dates <- !is.na(vn$start_datetime_var) && !is.na(vn$start_date_var)
  if (has_dates) {
    datetime_expr <- glue(
      "COALESCE(t.{vn$start_datetime_var}, ",
      "CAST(t.{vn$start_date_var} AS TIMESTAMP))")
    date_expr <- glue(
      "COALESCE(CAST(t.{vn$start_datetime_var} AS DATE), ",
      "t.{vn$start_date_var})")
    date_select <- glue(
      ",\n      {date_expr} AS table_date,",
      "\n      {datetime_expr} AS table_datetime")
  } else {
    date_select <- ""
  }

  # --- Optional date pre-filter ---
  # Filters on the mandatory date column (never NULL) using pre-computed bounds.
  # Bounds are computed in R with a ±1 day safety margin so no data is lost —
  # the exact window filter in the aggregation step enforces the real bounds.
  # Filtering on the raw date column (not COALESCEd) keeps the expression simple.
  date_filter <- ""
  if (has_dates && !is.null(lower_date) && !is.null(upper_date)) {
    date_filter <- glue(
      "\n      AND t.{vn$start_date_var} >= '{lower_date}'",
      "\n      AND t.{vn$start_date_var} <= '{upper_date}'")
  }

  # --- Separate filter types ---
  id_concepts <- table_concepts[
    !(table_concepts$omop_variable %in% c("concept_name", "concept_code",
                                          "ancestor_concept_id")) |
      is.na(table_concepts$omop_variable), ]
  ancestor_concepts <- table_concepts[
    table_concepts$omop_variable == "ancestor_concept_id" &
      !is.na(table_concepts$omop_variable), ]

  # Build one WHERE expression per filter type
  where_exprs <- character(0)
  if (nrow(id_concepts) > 0) {
    ids <- unique(id_concepts$concept_id)
    where_exprs <- c(where_exprs,
                     glue("{vn$concept_id_var} IN ({glue_collapse(ids, sep = ', ')})"))
  }
  if (nrow(ancestor_concepts) > 0) {
    anc_ids <- unique(ancestor_concepts$concept_id)
    where_exprs <- c(where_exprs,
                     glue("{vn$concept_id_var} IN (SELECT descendant_concept_id FROM ",
                          "@schema.concept_ancestor WHERE ancestor_concept_id IN (",
                          "{glue_collapse(anc_ids, sep = ', ')}))"))
  }
  if (has_string_ids) {
    where_exprs <- c(where_exprs,
                     glue("{vn$concept_id_var} IN (SELECT concept_id FROM ",
                          "string_resolved_ids WHERE table_name = '{table_name}')"))
  }

  # --- Column list and unit join ---
  has_numeric <- any(table_concepts$omop_variable == "value_as_number", na.rm = TRUE)
  has_val_concept <- any(table_concepts$omop_variable == "value_as_concept_id",
                         na.rm = TRUE)
  extra_cols <- unique(table_concepts$additional_filter_variable_name[
    !is.na(table_concepts$additional_filter_variable_name)])

  cols <- c("t.person_id", "t.visit_occurrence_id",
            glue("t.{vn$concept_id_var}"), glue("t.{vn$id_var}"))
  if (has_numeric) cols <- c(cols, "t.value_as_number", "t.unit_concept_id")
  if (has_val_concept) cols <- c(cols, "t.value_as_concept_id")
  if (length(extra_cols) > 0) cols <- c(cols, glue("t.{extra_cols}"))
  select_list <- glue_collapse(unique(cols), sep = ",\n      ")

  unit_join <- ""
  unit_select <- ""
  if (has_numeric) {
    unit_join <- paste0(
      "\n    LEFT JOIN @schema.concept c_unit",
      "\n      ON t.unit_concept_id = c_unit.concept_id",
      "\n      AND t.unit_concept_id IS NOT NULL")
    unit_select <- ",\n      c_unit.concept_name AS unit_name"
  }

  # --- Build the CREATE TEMP TABLE statement ---
  # Single filter type: simple WHERE clause.
  # Multiple filter types: UNION to let Postgres use indexes per branch.
  # OR across different filter strategies (IN-list vs subquery) causes Postgres 9
  # to fall back to sequential scans.

  build_select <- function(where_clause) {
    glue(
      "SELECT\n",
      "      {select_list}{date_select}{unit_select}\n",
      "    FROM @schema.{vn$db_table_name} t{unit_join}\n",
      "    WHERE t.person_id IN (SELECT person_id FROM person_batch)\n",
      "      AND {where_clause}{date_filter}")
  }

  if (length(where_exprs) == 0) {
    create_sql <- glue(
      "CREATE TEMP TABLE {temp_name} AS\n",
      build_select("false"))
  } else if (length(where_exprs) == 1) {
    create_sql <- glue(
      "CREATE TEMP TABLE {temp_name} AS\n",
      build_select(where_exprs[1]))
  } else {
    # UNION: one SELECT per filter type
    selects <- vapply(where_exprs, build_select, character(1))
    create_sql <- glue(
      "CREATE TEMP TABLE {temp_name} AS\n",
      paste(selects, collapse = "\nUNION\n"))
  }

  c(glue("DROP TABLE IF EXISTS {temp_name}"),
    create_sql,
    glue("ANALYZE {temp_name}"))
}


#' Build a SELECT for long-format numeric aggregation from temp tables.
#'
#' @param table_concepts Concepts (all types; filtered internally to numeric).
#' @param table_name OMOP table name.
#' @param variable_names Variable names dataframe.
#' @param window_start_point Windowing mode.
#' @param cadence Cadence in hours.
#'
#' @return A SELECT query string, or "".
#' @importFrom glue glue
#' @keywords internal
build_long_select <- function(table_concepts, table_name, variable_names,
                              window_start_point, cadence) {

  numeric <- table_concepts[table_concepts$omop_variable == "value_as_number" &
                              !is.na(table_concepts$omop_variable), ]
  if (nrow(numeric) == 0) return("")

  vn <- variable_names[variable_names$table == table_name, ]
  filtered_temp <- paste0(vn$alias, "_filtered_temp")

  window_expr <- if (table_name != "Visit Detail") {
    window_query(window_start_point, "t.table_datetime", "t.table_date", cadence)
  } else "0"

  filter_cols <- unique(numeric$additional_filter_variable_name[
    !is.na(numeric$additional_filter_variable_name)])
  extra_select <- if (length(filter_cols) > 0) {
    paste0(",\n      t.", filter_cols, collapse = "")
  } else ""
  extra_group <- if (length(filter_cols) > 0) {
    paste0(",\n      t.", filter_cols, collapse = "")
  } else ""

  # unit_name is pre-joined in the filtered temp table
  glue(
    "SELECT\n",
    "      t.person_id,\n",
    "      adm.icu_admission_datetime,\n",
    "      {window_expr} AS time_in_icu,\n",
    "      t.{vn$concept_id_var} AS concept_id,\n",
    "      MIN(t.value_as_number) AS min_val,\n",
    "      MAX(t.value_as_number) AS max_val,\n",
    "      MIN(t.unit_name) AS unit_name{extra_select}\n",
    "    FROM adm_multi_temp adm\n",
    "    INNER JOIN {filtered_temp} t\n",
    "      ON adm.person_id = t.person_id\n",
    "      AND (adm.visit_occurrence_id = t.visit_occurrence_id\n",
    "           OR (t.visit_occurrence_id IS NULL\n",
    "               AND t.table_datetime >= adm.hospital_admission_datetime\n",
    "               AND t.table_datetime <  adm.hospital_discharge_datetime))\n",
    "    WHERE {window_expr} >= @first_window\n",
    "      AND {window_expr} <= @last_window\n",
    "      AND t.value_as_number IS NOT NULL\n",
    "    GROUP BY\n",
    "      t.person_id, adm.icu_admission_datetime, {window_expr},\n",
    "      t.{vn$concept_id_var}{extra_group}")
}


#' Build a SELECT for count-based aggregation from temp tables.
#'
#' For tables with string search variables (concept_name/concept_code),
#' joins the concept table in the aggregation step — NOT in the filtered
#' temp table.
#'
#' @param table_concepts Concepts (all types; filtered internally to counts).
#' @param table_name OMOP table name.
#' @param variable_names Variable names dataframe.
#' @param window_start_point Windowing mode.
#' @param cadence Cadence in hours.
#'
#' @return A SELECT query string, or "".
#' @importFrom glue glue
#' @keywords internal
build_count_select <- function(table_concepts, table_name, variable_names,
                               window_start_point, cadence,
                               has_string_ids = FALSE,
                               string_short_names = character(0)) {

  counts <- table_concepts[
    table_concepts$omop_variable != "value_as_number" |
      is.na(table_concepts$omop_variable), ]

  # If no regular count variables and no string search variables, nothing to do
  if (nrow(counts) == 0 && length(string_short_names) == 0) return("")

  vn <- variable_names[variable_names$table == table_name, ]
  filtered_temp <- paste0(vn$alias, "_filtered_temp")

  window_expr <- if (table_name != "Visit Detail") {
    window_query(window_start_point, "t.table_datetime", "t.table_date", cadence)
  } else "0"

  variables <- if (nrow(counts) > 0) {
    variables_query(counts, vn$concept_id_var, vn$id_var)
  } else ""

  # String search count variables: one COUNT per short_name using temp table
  string_sql_parts <- character(0)
  if (has_string_ids && length(string_short_names) > 0) {
    for (sn in string_short_names) {
      string_sql_parts <- c(string_sql_parts, glue(
        ", COUNT(CASE WHEN t.{vn$concept_id_var} IN ",
        "(SELECT concept_id FROM string_resolved_ids ",
        "WHERE table_name = '{table_name}' AND short_name = '{sn}') ",
        "THEN t.{vn$id_var} END) AS count_{sn}"))
    }
  }
  string_variables <- paste(string_sql_parts, collapse = "\n")

  all_variables <- paste0(variables, string_variables)
  if (all_variables == "") return("")
  all_variables <- sub("^\\s*,\\s*", "", all_variables)

  join_condition <- if (table_name == "Visit Detail") {
    glue("ON adm.person_id = t.person_id\n",
         "      AND adm.visit_occurrence_id = t.visit_occurrence_id")
  } else {
    glue("ON adm.person_id = t.person_id\n",
         "      AND (adm.visit_occurrence_id = t.visit_occurrence_id\n",
         "           OR (t.visit_occurrence_id IS NULL\n",
         "               AND t.table_datetime >= adm.hospital_admission_datetime\n",
         "               AND t.table_datetime <  adm.hospital_discharge_datetime))")
  }

  glue(
    "SELECT\n",
    "       t.person_id,\n",
    "       adm.icu_admission_datetime,\n",
    "       {window_expr} AS time_in_icu, \n",
    "       {all_variables}\n",
    "    FROM adm_multi_temp adm\n",
    "    INNER JOIN {filtered_temp} t\n",
    "      {join_condition}\n",
    "    WHERE {window_expr} >= @first_window\n",
    "      AND {window_expr} <= @last_window\n",
    "    GROUP BY\n",
    "      t.person_id, adm.icu_admission_datetime, {window_expr}")
}


#' Build Drug table statements with pre-resolved ancestor map.
#'
#' The drug table uses ancestor_concept_id to identify drug groups (e.g.
#' antibiotics, vasoactives). Instead of correlated subqueries against
#' concept_ancestor in the WHERE and CASE WHEN expressions, we:
#'   1. Create ancestor_map: resolves all ancestor IDs to descendant IDs
#'      with boolean flag columns per drug group. Created once.
#'   2. Create drg_filtered_temp: filter drug_exposure by person_batch
#'      and join to ancestor_map to get drug group flags.
#'   3. Aggregate using the pre-computed flags instead of subqueries.
#'
#' This is 10-100x faster than correlated subqueries (0.8s vs 22s+).
#'
#' @param concepts Full concepts dataframe.
#' @param variable_names Variable names dataframe.
#' @param window_start_point Windowing mode.
#' @param cadence Cadence in hours.
#' @param dialect SQL dialect.
#'
#' @return A list with \code{setup_stmts} (CREATE ancestor_map + drg_filtered +
#'   drg_tagged), and \code{select_sql} (aggregation SELECT), or NULL.
#' @importFrom glue glue glue_collapse
#' @keywords internal
build_drug_statements <- function(concepts, variable_names,
                                  window_start_point, cadence, dialect,
                                  has_string_ids = FALSE,
                                  string_short_names = character(0)) {

  drug <- concepts[concepts$table == "Drug", ]
  # If no regular drug concepts and no string search drugs, nothing to do
  if (nrow(drug) == 0 && !has_string_ids) return(NULL)

  vn <- variable_names[variable_names$table == "Drug", ]

  ws <- window_query(window_start_point, "i.table_datetime", "i.table_date", cadence)
  we <- window_query(window_start_point, "i.table_end_datetime", "i.table_end_date", cadence)

  drug_join <- translate_drug_join(dialect)

  # --- Identify drug groups from concepts ---
  ancestor_drugs <- drug[drug$omop_variable == "ancestor_concept_id" &
                           !is.na(drug$omop_variable), ]
  non_ancestor_drugs <- drug[drug$omop_variable != "ancestor_concept_id" |
                               is.na(drug$omop_variable), ]

  all_ancestor_ids <- unique(ancestor_drugs$concept_id)

  # Separate ancestor_map statements (run once) from per-batch statements
  ancestor_stmts <- character(0)
  batch_stmts <- character(0)

  if (nrow(ancestor_drugs) > 0) {
    flag_exprs <- ancestor_drugs %>%
      group_by(short_name) %>%
      summarise(
        anc_ids = glue_collapse(unique(concept_id), sep = ", "),
        .groups = "drop") %>%
      mutate(
        flag = glue("MAX(CASE WHEN ancestor_concept_id IN ({anc_ids}) ",
                    "THEN 1 ELSE 0 END) AS is_{short_name}"))
    flag_sql <- glue_collapse(flag_exprs$flag, sep = ",\n    ")

    ancestor_stmts <- c(
      "DROP TABLE IF EXISTS ancestor_map",
      glue(
        "CREATE TEMP TABLE ancestor_map AS\n",
        "SELECT DISTINCT descendant_concept_id,\n",
        "    {flag_sql}\n",
        "  FROM @schema.concept_ancestor\n",
        "  WHERE ancestor_concept_id IN ({glue_collapse(all_ancestor_ids, sep = ', ')})\n",
        "  GROUP BY descendant_concept_id"),
      "ANALYZE ancestor_map")
  }

  # --- Step 2: drg_filtered_temp ---
  direct_id_drugs <- non_ancestor_drugs[
    !(non_ancestor_drugs$omop_variable %in% c("concept_name", "concept_code")) |
      is.na(non_ancestor_drugs$omop_variable), ]

  where_parts <- character(0)
  if (nrow(ancestor_drugs) > 0)
    where_parts <- c(where_parts,
                     "drug_concept_id IN (SELECT descendant_concept_id FROM ancestor_map)")
  if (nrow(direct_id_drugs) > 0)
    where_parts <- c(where_parts,
                     glue("drug_concept_id IN ({glue_collapse(unique(direct_id_drugs$concept_id), sep = ', ')})"))
  if (has_string_ids)
    where_parts <- c(where_parts,
                     "drug_concept_id IN (SELECT concept_id FROM string_resolved_ids WHERE table_name = 'Drug')")

  # Only join concept table if non-ancestor drugs need concept_name/concept_code
  # columns for count CASE WHEN expressions (e.g. concept_code searches).
  # After string resolution, most drugs don't need this join.
  needs_concept_join <- nrow(non_ancestor_drugs) > 0

  if (needs_concept_join) {
    concept_cols <- ",\n  c.concept_name, c.concept_code"
    concept_join <- "\nINNER JOIN @schema.concept c ON c.concept_id = t.drug_concept_id"
  } else {
    concept_cols <- ""
    concept_join <- ""
  }

  # Build SELECT template for drug filtered temp
  drg_select <- function(where_clause) {
    glue(
      "SELECT\n",
      "  t.person_id, t.visit_occurrence_id, t.drug_exposure_id, t.drug_concept_id{concept_cols},\n",
      "  COALESCE(CAST(t.{vn$start_datetime_var} AS DATE), t.{vn$start_date_var}) AS table_date,\n",
      "  COALESCE(t.{vn$start_datetime_var}, CAST(t.{vn$start_date_var} AS TIMESTAMP)) AS table_datetime,\n",
      "  COALESCE(CAST(t.{vn$end_datetime_var} AS DATE), t.{vn$end_date_var}) AS table_end_date,\n",
      "  COALESCE(t.{vn$end_datetime_var}, CAST(t.{vn$end_date_var} AS TIMESTAMP)) AS table_end_datetime\n",
      "FROM @schema.drug_exposure t{concept_join}\n",
      "WHERE t.person_id IN (SELECT person_id FROM person_batch)\n",
      "  AND {where_clause}")
  }

  if (length(where_parts) == 0) {
    drg_create <- glue("CREATE TEMP TABLE drg_filtered_temp AS\n", drg_select("false"))
  } else if (length(where_parts) == 1) {
    drg_create <- glue("CREATE TEMP TABLE drg_filtered_temp AS\n", drg_select(where_parts[1]))
  } else {
    selects <- vapply(where_parts, drg_select, character(1))
    drg_create <- glue(
      "CREATE TEMP TABLE drg_filtered_temp AS\n",
      paste(selects, collapse = "\nUNION\n"))
  }

  batch_stmts <- c(batch_stmts,
                   "DROP TABLE IF EXISTS drg_filtered_temp",
                   drg_create,
                   "ANALYZE drg_filtered_temp")

  # --- Step 3: drg_tagged (join ancestor flags) ---
  if (nrow(ancestor_drugs) > 0) {
    flag_cols <- glue_collapse(glue(
      "COALESCE(a.is_{unique(ancestor_drugs$short_name)}, 0) AS is_{unique(ancestor_drugs$short_name)}"),
      sep = ",\n    ")

    batch_stmts <- c(batch_stmts,
                     "DROP TABLE IF EXISTS drg_tagged",
                     glue(
                       "CREATE TEMP TABLE drg_tagged AS\n",
                       "SELECT d.*,\n",
                       "    {flag_cols}\n",
                       "  FROM drg_filtered_temp d\n",
                       "  LEFT JOIN ancestor_map a ON d.drug_concept_id = a.descendant_concept_id"),
                     "ANALYZE drg_tagged")
    agg_table <- "drg_tagged"
  } else {
    agg_table <- "drg_filtered_temp"
  }

  # --- Step 4: aggregation SELECT ---
  count_parts <- character(0)

  if (nrow(ancestor_drugs) > 0) {
    for (sn in unique(ancestor_drugs$short_name)) {
      count_parts <- c(count_parts,
                       glue(", COUNT(CASE WHEN is_{sn} = 1 THEN drug_exposure_id END) AS count_{sn}"))
    }
  }

  if (nrow(non_ancestor_drugs) > 0) {
    non_anc_vars <- variables_query(non_ancestor_drugs, vn$concept_id_var, vn$id_var)
    if (non_anc_vars != "") count_parts <- c(count_parts, non_anc_vars)
  }

  # String search drug count variables
  if (has_string_ids && length(string_short_names) > 0) {
    for (sn in string_short_names) {
      count_parts <- c(count_parts, glue(
        ", COUNT(CASE WHEN drug_concept_id IN ",
        "(SELECT concept_id FROM string_resolved_ids ",
        "WHERE table_name = 'Drug' AND short_name = '{sn}') ",
        "THEN drug_exposure_id END) AS count_{sn}"))
    }
  }

  variables_sql <- paste(count_parts, collapse = "\n")

  # Build inner SELECT column list
  inner_cols <- "i.drug_exposure_id, i.drug_concept_id"
  if (needs_concept_join) {
    inner_cols <- paste0(inner_cols, ",\n           i.concept_name, i.concept_code")
  }
  if (nrow(ancestor_drugs) > 0) {
    inner_cols <- paste0(inner_cols, ",\n           ",
                         paste(glue("i.is_{unique(ancestor_drugs$short_name)}"),
                               collapse = ", "))
  }

  select_sql <- glue(
    "SELECT t.person_id, t.icu_admission_datetime, gs.time_in_icu\n",
    "    {variables_sql}\n",
    "FROM (\n",
    "    SELECT adm.person_id, adm.icu_admission_datetime,\n",
    "           {inner_cols},\n",
    "           {ws} AS drug_start, {we} AS drug_end\n",
    "    FROM adm_multi_temp adm\n",
    "    INNER JOIN {agg_table} i\n",
    "      ON adm.person_id = i.person_id\n",
    "      AND (adm.visit_occurrence_id = i.visit_occurrence_id\n",
    "           OR (i.visit_occurrence_id IS NULL\n",
    "               AND i.table_datetime >= adm.hospital_admission_datetime\n",
    "               AND i.table_datetime <  adm.hospital_discharge_datetime))\n",
    "      AND ({ws} >= @first_window OR {we} >= @first_window)\n",
    "      AND ({ws} <= @last_window  OR {we} <= @last_window)\n",
    ") t\n",
    "{drug_join}\n",
    "GROUP BY t.person_id, t.icu_admission_datetime, gs.time_in_icu")

  list(ancestor_stmts = ancestor_stmts,
       batch_stmts = batch_stmts,
       select_sql = select_sql)
}
