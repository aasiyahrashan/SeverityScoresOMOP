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
#' Does NOT join the concept table — that happens in the aggregation step.
#' Pre-joins unit names (LEFT JOIN concept on unit_concept_id) for tables
#' with numeric variables, since this is needed by the long-format aggregation.
#'
#' @param table_concepts Concepts dataframe, pre-filtered to this table.
#' @param table_name OMOP table name.
#' @param variable_names Variable names dataframe.
#'
#' @return Character vector: DROP, CREATE TEMP TABLE, ANALYZE statements.
#' @importFrom glue glue glue_collapse
#' @keywords internal
build_filtered_temp <- function(table_concepts, table_name, variable_names) {

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

  # WHERE clause: concept_id filter + optional string search.
  # String search (concept_name LIKE ...) requires joining the concept table.
  # We only add that join when string search variables exist for this table.
  has_string_search <- any(table_concepts$omop_variable %in%
                             c("concept_name", "concept_code"), na.rm = TRUE)

  # Concept_id-based filter (fast, uses index)
  id_concepts <- table_concepts[
    !(table_concepts$omop_variable %in% c("concept_name", "concept_code")) |
      is.na(table_concepts$omop_variable), ]
  ancestor_concepts <- table_concepts[
    table_concepts$omop_variable == "ancestor_concept_id" &
      !is.na(table_concepts$omop_variable), ]

  expressions <- character(0)
  if (nrow(id_concepts) > 0) {
    ids <- unique(id_concepts$concept_id)
    expressions <- c(expressions,
                     glue("{vn$concept_id_var} IN ({glue_collapse(ids, sep = ', ')})"))
  }
  if (nrow(ancestor_concepts) > 0) {
    anc_ids <- unique(ancestor_concepts$concept_id)
    expressions <- c(expressions,
                     glue("{vn$concept_id_var} IN (SELECT descendant_concept_id FROM ",
                          "@schema.concept_ancestor WHERE ancestor_concept_id IN (",
                          "{glue_collapse(anc_ids, sep = ', ')}))"))
  }

  # String search filter (requires concept join)
  if (has_string_search) {
    str_concepts <- table_concepts[
      table_concepts$omop_variable %in% c("concept_name", "concept_code") &
        !is.na(table_concepts$omop_variable), ]
    str_exprs <- str_concepts %>%
      distinct(omop_variable, concept_id) %>%
      reframe(expr = glue(
        "LOWER(c_str.{omop_variable}) LIKE '%{tolower(concept_id)}%'")) %>%
      pull(expr)
    expressions <- c(expressions, str_exprs)
  }

  if (length(expressions) == 0) {
    where_expr <- "false"
  } else {
    where_expr <- paste0("(", paste(expressions, collapse = " OR "), ")")
  }

  # Concept join for string search (only when needed)
  string_join <- ""
  if (has_string_search) {
    string_join <- paste0(
      "\n    INNER JOIN @schema.concept c_str",
      "\n      ON c_str.concept_id = t.", vn$concept_id_var)
  }

  # Column list
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

  # Pre-join unit names for numeric variables (LEFT JOIN, not INNER JOIN)
  unit_join <- ""
  unit_select <- ""
  if (has_numeric) {
    unit_join <- paste0(
      "\n    LEFT JOIN @schema.concept c_unit",
      "\n      ON t.unit_concept_id = c_unit.concept_id",
      "\n      AND t.unit_concept_id IS NOT NULL")
    unit_select <- ",\n      c_unit.concept_name AS unit_name"
  }

  # Include concept_name/concept_code when string search variables exist
  string_select <- ""
  if (has_string_search) {
    string_select <- ",\n      c_str.concept_name, c_str.concept_code"
  }

  create_sql <- glue(
    "CREATE TEMP TABLE {temp_name} AS\n",
    "SELECT\n",
    "      {select_list}{date_select}{unit_select}{string_select}\n",
    "    FROM @schema.{vn$db_table_name} t{unit_join}{string_join}\n",
    "    WHERE t.person_id IN (SELECT person_id FROM person_batch)\n",
    "      AND {where_expr}")

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
                               window_start_point, cadence) {

  counts <- table_concepts[
    table_concepts$omop_variable != "value_as_number" |
      is.na(table_concepts$omop_variable), ]
  if (nrow(counts) == 0) return("")

  vn <- variable_names[variable_names$table == table_name, ]
  filtered_temp <- paste0(vn$alias, "_filtered_temp")

  window_expr <- if (table_name != "Visit Detail") {
    window_query(window_start_point, "t.table_datetime", "t.table_date", cadence)
  } else "0"

  variables <- variables_query(counts, vn$concept_id_var, vn$id_var)

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
    "       {window_expr} AS time_in_icu\n",
    "       {variables}\n",
    "    FROM adm_multi_temp adm\n",
    "    INNER JOIN {filtered_temp} t\n",
    "      {join_condition}\n",
    "    WHERE {window_expr} >= @first_window\n",
    "      AND {window_expr} <= @last_window\n",
    "    GROUP BY\n",
    "      t.person_id, adm.icu_admission_datetime, {window_expr}")
}


#' Build Drug table statements (filtered temp + aggregation SELECT).
#'
#' @param concepts Full concepts dataframe.
#' @param variable_names Variable names dataframe.
#' @param window_start_point Windowing mode.
#' @param cadence Cadence in hours.
#' @param dialect SQL dialect.
#'
#' @return A list with filtered_stmts and select_sql, or NULL.
#' @importFrom glue glue
#' @keywords internal
build_drug_statements <- function(concepts, variable_names,
                                  window_start_point, cadence, dialect) {

  drug <- concepts[concepts$table == "Drug", ]
  if (nrow(drug) == 0) return(NULL)

  vn <- variable_names[variable_names$table == "Drug", ]

  ws <- window_query(window_start_point, "i.table_datetime", "i.table_date", cadence)
  we <- window_query(window_start_point, "i.table_end_datetime", "i.table_end_date", cadence)

  variables <- variables_query(drug, vn$concept_id_var, vn$id_var)
  where_expr <- where_clause(drug, variable_names, "Drug")
  drug_join <- translate_drug_join(dialect)

  filtered_stmts <- c(
    "DROP TABLE IF EXISTS drg_filtered_temp",
    glue(
      "CREATE TEMP TABLE drg_filtered_temp AS\n",
      "SELECT\n",
      "  t.person_id, t.visit_occurrence_id, t.drug_exposure_id, t.drug_concept_id,\n",
      "  c.concept_name, c.concept_code,\n",
      "  COALESCE(CAST(t.{vn$start_datetime_var} AS DATE), t.{vn$start_date_var}) AS table_date,\n",
      "  COALESCE(t.{vn$start_datetime_var}, CAST(t.{vn$start_date_var} AS TIMESTAMP)) AS table_datetime,\n",
      "  COALESCE(CAST(t.{vn$end_datetime_var} AS DATE), t.{vn$end_date_var}) AS table_end_date,\n",
      "  COALESCE(t.{vn$end_datetime_var}, CAST(t.{vn$end_date_var} AS TIMESTAMP)) AS table_end_datetime\n",
      "FROM @schema.drug_exposure t\n",
      "INNER JOIN @schema.concept c ON c.concept_id = t.drug_concept_id\n",
      "WHERE t.person_id IN (SELECT person_id FROM person_batch)\n",
      "  AND {where_expr}"),
    "ANALYZE drg_filtered_temp")

  select_sql <- glue(
    "SELECT t.person_id, t.icu_admission_datetime, gs.time_in_icu\n",
    "    {variables}\n",
    "FROM (\n",
    "    SELECT adm.person_id, adm.icu_admission_datetime,\n",
    "           i.drug_exposure_id, i.drug_concept_id,\n",
    "           i.concept_name, i.concept_code,\n",
    "           {ws} AS drug_start, {we} AS drug_end\n",
    "    FROM adm_multi_temp adm\n",
    "    INNER JOIN drg_filtered_temp i\n",
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

  list(filtered_stmts = filtered_stmts, select_sql = select_sql)
}
