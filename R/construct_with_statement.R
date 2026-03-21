# =============================================================================
# construct_with_statement.R
#
# Builds SQL statements that operate on pre-materialised temp tables:
#   - person_batch (patient IDs for this batch)
#   - adm_multi_temp (icu_admission_details_multiple_visits)
#
# Every function returns a simple query — no CTE preambles, no pasted visits.
# The temp tables are created once per batch in run_query_function.R.
# =============================================================================


#' Build a CREATE TEMP TABLE statement for the filtered clinical data.
#'
#' Materialises filtered rows from one OMOP clinical table into a temp table.
#' Filters by person_batch (IN subquery) and concept IDs. On Postgres 9,
#' IN (subquery) lets the planner choose between person_id and concept_id
#' indexes — typically 10-20x faster than JOIN on large tables.
#'
#' @param table_concepts Concepts dataframe, pre-filtered to this table.
#' @param table_name OMOP table name.
#' @param variable_names Variable names dataframe.
#'
#' @return A character vector of SQL statements: CREATE TEMP TABLE, ANALYZE.
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
    date_select <- glue(",\n      {date_expr} AS table_date,\n      {datetime_expr} AS table_datetime")
  } else {
    date_select <- ""
  }

  where_expr <- where_clause(table_concepts, variable_names, table_name)

  needs_concept_join <- any(
    table_concepts$omop_variable %in% c("concept_name", "concept_code"),
    na.rm = TRUE)
  concept_join <- if (needs_concept_join) {
    glue("INNER JOIN @schema.concept c ON c.concept_id = t.{vn$concept_id_var}")
  } else ""

  # Minimal column list
  has_numeric <- any(table_concepts$omop_variable == "value_as_number", na.rm = TRUE)
  has_val_concept <- any(table_concepts$omop_variable == "value_as_concept_id", na.rm = TRUE)
  extra_cols <- unique(table_concepts$additional_filter_variable_name[
    !is.na(table_concepts$additional_filter_variable_name)])

  cols <- c("t.person_id", "t.visit_occurrence_id",
            glue("t.{vn$concept_id_var}"), glue("t.{vn$id_var}"))
  if (has_numeric)     cols <- c(cols, "t.value_as_number", "t.unit_concept_id")
  if (has_val_concept) cols <- c(cols, "t.value_as_concept_id")
  if (length(extra_cols) > 0) cols <- c(cols, glue("t.{extra_cols}"))

  select_list <- glue_collapse(unique(cols), sep = ",\n      ")
  concept_cols <- if (needs_concept_join) ", c.concept_name, c.concept_code" else ""

  create_sql <- glue(
    "CREATE TEMP TABLE {temp_name} AS\n",
    "SELECT\n",
    "      {select_list}{date_select}\n",
    "      {concept_cols}\n",
    "    FROM @schema.{vn$db_table_name} t\n",
    "    {concept_join}\n",
    "    WHERE t.person_id IN (SELECT person_id FROM person_batch)\n",
    "      AND ({where_expr})")

  c(glue("DROP TABLE IF EXISTS {temp_name}"),
    create_sql,
    glue("ANALYZE {temp_name}"))
}


#' Build a SELECT for long-format numeric aggregation from temp tables.
#'
#' References adm_multi_temp and {alias}_filtered_temp directly — no CTEs.
#'
#' @param table_concepts Concepts (all types; filtered internally).
#' @param table_name OMOP table name.
#' @param variable_names Variable names dataframe.
#' @param window_start_point Windowing mode.
#' @param cadence Cadence in hours.
#'
#' @return A SELECT query string, or "" if no numeric variables.
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

  glue(
    "SELECT\n",
    "      t.person_id,\n",
    "      adm.icu_admission_datetime,\n",
    "      {window_expr} AS time_in_icu,\n",
    "      t.{vn$concept_id_var} AS concept_id,\n",
    "      MIN(t.value_as_number) AS min_val,\n",
    "      MAX(t.value_as_number) AS max_val,\n",
    "      MIN(c_unit.concept_name) AS unit_name{extra_select}\n",
    "    FROM adm_multi_temp adm\n",
    "    INNER JOIN {filtered_temp} t\n",
    "      ON adm.person_id = t.person_id\n",
    "      AND (adm.visit_occurrence_id = t.visit_occurrence_id\n",
    "           OR (t.visit_occurrence_id IS NULL\n",
    "               AND t.table_datetime >= adm.hospital_admission_datetime\n",
    "               AND t.table_datetime <  adm.hospital_discharge_datetime))\n",
    "    LEFT JOIN @schema.concept c_unit\n",
    "      ON t.unit_concept_id = c_unit.concept_id\n",
    "      AND t.unit_concept_id IS NOT NULL\n",
    "    WHERE {window_expr} >= @first_window\n",
    "      AND {window_expr} <= @last_window\n",
    "      AND t.value_as_number IS NOT NULL\n",
    "    GROUP BY\n",
    "      t.person_id, adm.icu_admission_datetime, {window_expr},\n",
    "      t.{vn$concept_id_var}{extra_group}")
}


#' Build a SELECT for count-based aggregation from temp tables.
#'
#' @param table_concepts Concepts (all types; filtered internally).
#' @param table_name OMOP table name.
#' @param variable_names Variable names dataframe.
#' @param window_start_point Windowing mode.
#' @param cadence Cadence in hours.
#'
#' @return A SELECT query string, or "" if no count variables.
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


#' Build the Drug table query from temp tables.
#'
#' Drug uses generate_series / OUTER APPLY for date range expansion.
#' References adm_multi_temp and drg_filtered_temp.
#'
#' @param concepts Full concepts dataframe.
#' @param variable_names Variable names dataframe.
#' @param window_start_point Windowing mode.
#' @param cadence Cadence in hours.
#' @param dialect SQL dialect.
#'
#' @return A list with \code{filtered_stmts} (CREATE/ANALYZE statements)
#'   and \code{select_sql} (the aggregation SELECT), or NULL if no Drug concepts.
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

  # Filtered temp table for drugs
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
