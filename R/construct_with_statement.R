# =============================================================================
# construct_with_statement.R
#
# Builds standalone SQL queries for each OMOP clinical table. Each query
# returns physiology data for one table as a self-contained result set.
# R merges everything together after execution.
#
# Architecture:
#   - Each non-Drug table produces up to two queries:
#       1. A long-format query for value_as_number variables (if any)
#       2. A count query for non-numeric variables (if any)
#   - The Drug table produces one count query (uses generate_series)
#   - All queries share the same CTE preamble (pasted visits) and reuse
#     the person_batch temp table.
#
# Why separate queries instead of one big WITH block:
#   - Postgres 9 CTEs are optimization fences — chaining many CTEs causes
#     the planner to hang on CTE-to-CTE joins, especially with OR conditions.
#   - Each query is simple and focused, making debugging and profiling easy.
#   - R does the joining (instant on aggregated data), so there's no penalty.
# =============================================================================


#' Build a _filtered CTE string for a non-Drug OMOP table.
#'
#' Materialises a small intermediate result by filtering the clinical table
#' by person_batch and concept IDs. Uses IN (SELECT ...) rather than
#' INNER JOIN for person filtering — on Postgres 9, this lets the planner
#' choose between the person_id and concept_id indexes.
#'
#' @param table_concepts Concepts dataframe, already filtered to this table.
#' @param table_name OMOP table name (e.g. "Measurement").
#' @param variable_names The full variable_names dataframe.
#'
#' @return A SQL CTE string starting with ", {alias}_filtered AS (...)".
#' @keywords internal
filtered_cte <- function(table_concepts, table_name, variable_names) {

  vn <- variable_names %>%
    filter(table == table_name)

  # Build datetime expressions
  datetime_expr <- glue(
    "COALESCE(t.{vn$start_datetime_var}, ",
    "CAST(t.{vn$start_date_var} AS TIMESTAMP))")
  date_expr <- glue(
    "COALESCE(CAST(t.{vn$start_datetime_var} AS DATE), ",
    "t.{vn$start_date_var})")

  # Where clause
  where_expression <- where_clause(table_concepts, variable_names, table_name)

  # Concept join only for string search variables
  needs_concept_join <- any(
    table_concepts$omop_variable %in% c("concept_name", "concept_code"),
    na.rm = TRUE)
  concept_join <- if (needs_concept_join) {
    glue("INNER JOIN @schema.concept c\n",
         "  ON c.concept_id = t.{vn$concept_id_var}")
  } else {
    ""
  }

  # Explicit column list — only what downstream CTEs need
  has_numeric <- any(table_concepts$omop_variable == "value_as_number",
                     na.rm = TRUE)
  has_value_as_concept <- any(
    table_concepts$omop_variable == "value_as_concept_id", na.rm = TRUE)
  additional_cols <- unique(table_concepts$additional_filter_variable_name[
    !is.na(table_concepts$additional_filter_variable_name)])

  cols <- c(
    "t.person_id",
    "t.visit_occurrence_id",
    glue("t.{vn$concept_id_var}"),
    glue("t.{vn$id_var}")
  )
  if (has_numeric) {
    cols <- c(cols, "t.value_as_number", "t.unit_concept_id")
  }
  if (has_value_as_concept) {
    cols <- c(cols, "t.value_as_concept_id")
  }
  if (length(additional_cols) > 0) {
    cols <- c(cols, glue("t.{additional_cols}"))
  }
  select_list <- glue_collapse(unique(cols), sep = ",\n      ")

  glue("
    , {vn$alias}_filtered AS (
    SELECT
      {select_list},
      {date_expr} AS table_date,
      {datetime_expr} AS table_datetime
      {if (needs_concept_join) ', c.concept_name, c.concept_code' else ''}
    FROM @schema.{vn$db_table_name} t
    {concept_join}
    WHERE t.person_id IN (SELECT person_id FROM person_batch)
      AND ({where_expression}))")
}


#' Build a complete long-format query for value_as_number variables in one table.
#'
#' Returns a self-contained SQL query (pasted visits + _filtered + _long + SELECT).
#' The result has columns: person_id, icu_admission_datetime, time_in_icu,
#' concept_id, min_val, max_val, unit_name, plus any additional filter columns.
#'
#' @param table_concepts Concepts for this table, pre-filtered to value_as_number.
#' @param table_name OMOP table name.
#' @param variable_names Variable names dataframe.
#' @param pasted_visits_sql Pre-rendered pasted visits SQL.
#' @param window_start_point Windowing mode.
#' @param cadence Cadence in hours.
#'
#' @return A SQL string for the complete long-format query, or "" if no
#'   numeric variables exist.
#' @keywords internal
build_long_query <- function(table_concepts, table_name, variable_names,
                             pasted_visits_sql, window_start_point, cadence) {

  numeric_concepts <- table_concepts %>%
    filter(omop_variable == "value_as_number")

  if (nrow(numeric_concepts) == 0) return("")

  vn <- variable_names %>% filter(table == table_name)
  alias_long <- paste0(vn$alias, "_long")

  # Windowing
  if (table_name != "Visit Detail") {
    window_expr <- window_query(window_start_point,
                                "t.table_datetime", "t.table_date",
                                cadence)
  } else {
    window_expr <- 0
  }

  # Additional filter columns
  has_additional_filter <- any(
    !is.na(numeric_concepts$additional_filter_variable_name))
  additional_select <- ""
  additional_group_by <- ""
  if (has_additional_filter) {
    filter_cols <- unique(numeric_concepts$additional_filter_variable_name[
      !is.na(numeric_concepts$additional_filter_variable_name)])
    additional_select <- paste0(",\n      t.", filter_cols, collapse = "")
    additional_group_by <- paste0(",\n      t.", filter_cols, collapse = "")
  }

  # Build _filtered CTE (only includes numeric concepts for this query)
  filt <- filtered_cte(numeric_concepts, table_name, variable_names)

  long_cte <- glue("
    , {alias_long} AS (
    SELECT
      t.person_id,
      adm.icu_admission_datetime,
      {window_expr} AS time_in_icu,
      t.{vn$concept_id_var} AS concept_id,
      MIN(t.value_as_number) AS min_val,
      MAX(t.value_as_number) AS max_val,
      MIN(c_unit.concept_name) AS unit_name{additional_select}
    FROM icu_admission_details_multiple_visits adm
    INNER JOIN {vn$alias}_filtered t
      ON adm.person_id = t.person_id
      AND (
         adm.visit_occurrence_id = t.visit_occurrence_id
         OR (t.visit_occurrence_id IS NULL
             AND t.table_datetime >= adm.hospital_admission_datetime
             AND t.table_datetime <  adm.hospital_discharge_datetime)
        )
    LEFT JOIN @schema.concept c_unit
      ON t.unit_concept_id = c_unit.concept_id
      AND t.unit_concept_id IS NOT NULL
    WHERE {window_expr} >= @first_window
      AND {window_expr} <= @last_window
      AND t.value_as_number IS NOT NULL
    GROUP BY
      t.person_id,
      adm.icu_admission_datetime,
      {window_expr},
      t.{vn$concept_id_var}{additional_group_by})")

  glue("{pasted_visits_sql}\n{filt}\n{long_cte}\nSELECT * FROM {alias_long};")
}


#' Build a complete count query for non-numeric variables in one table.
#'
#' Returns a self-contained SQL query (pasted visits + _filtered + _counts + SELECT).
#' The result has columns: person_id, icu_admission_datetime, time_in_icu,
#' plus count_{short_name} columns.
#'
#' @param table_concepts Concepts for this table, pre-filtered to non-numeric types.
#' @param table_name OMOP table name.
#' @param variable_names Variable names dataframe.
#' @param pasted_visits_sql Pre-rendered pasted visits SQL.
#' @param window_start_point Windowing mode.
#' @param cadence Cadence in hours.
#'
#' @return A SQL string for the complete count query, or "" if no count
#'   variables exist.
#' @keywords internal
build_count_query <- function(table_concepts, table_name, variable_names,
                              pasted_visits_sql, window_start_point, cadence) {

  count_concepts <- table_concepts %>%
    filter(omop_variable != "value_as_number" | is.na(omop_variable))

  if (nrow(count_concepts) == 0) return("")

  vn <- variable_names %>% filter(table == table_name)
  alias_counts <- paste0(vn$alias, "_counts")

  # Windowing
  if (table_name != "Visit Detail") {
    window_expr <- window_query(window_start_point,
                                "t.table_datetime", "t.table_date",
                                cadence)
  } else {
    window_expr <- 0
  }

  # CASE WHEN expressions
  variables <- variables_query(count_concepts,
                               vn$concept_id_var,
                               vn$id_var)

  # Build _filtered CTE (only includes count concepts for this query)
  filt <- filtered_cte(count_concepts, table_name, variable_names)

  count_cte <- glue("
    , {alias_counts} AS (
    SELECT
       t.person_id,
       adm.icu_admission_datetime,
       {window_expr} AS time_in_icu
       {variables}
    FROM icu_admission_details_multiple_visits adm
    INNER JOIN {vn$alias}_filtered t
      ON adm.person_id = t.person_id
      AND (
         adm.visit_occurrence_id = t.visit_occurrence_id
         OR (t.visit_occurrence_id IS NULL
             AND t.table_datetime >= adm.hospital_admission_datetime
             AND t.table_datetime <  adm.hospital_discharge_datetime)
        )
    WHERE {window_expr} >= @first_window
      AND {window_expr} <= @last_window
    GROUP BY
      t.person_id,
      adm.icu_admission_datetime,
      {window_expr})")

  glue("{pasted_visits_sql}\n{filt}\n{count_cte}\nSELECT * FROM {alias_counts};")
}


#' Build the Drug table query.
#'
#' The Drug table uses generate_series/OUTER APPLY to expand date ranges
#' into time windows. Returns a self-contained query.
#'
#' @param concepts Full concepts dataframe.
#' @param variable_names Variable names dataframe.
#' @param pasted_visits_sql Pre-rendered pasted visits SQL.
#' @param window_start_point Windowing mode.
#' @param cadence Cadence in hours.
#' @param dialect SQL dialect ("postgresql" or "sql server").
#'
#' @return SQL string for the complete drug query, or "" if no Drug concepts.
#' @keywords internal
build_drug_query <- function(concepts, variable_names, pasted_visits_sql,
                             window_start_point, cadence, dialect) {

  drug_concepts <- concepts %>% filter(table == "Drug")
  if (nrow(drug_concepts) == 0) return("")

  vn <- variable_names %>% filter(table == "Drug")

  # Window queries
  window_start <- window_query(window_start_point,
                               "i.table_datetime", "i.table_date", cadence)
  window_end <- window_query(window_start_point,
                             "i.table_end_datetime", "i.table_end_date", cadence)

  # CASE WHEN expressions
  variables <- variables_query(drug_concepts, vn$concept_id_var, vn$id_var)
  where_expression <- where_clause(drug_concepts, variable_names, "Drug")
  drug_join <- translate_drug_join(dialect)

  drug_sql <- glue("
    {pasted_visits_sql}

    , drg_filtered AS (
    SELECT
    t.person_id,
    t.visit_occurrence_id,
    t.drug_exposure_id,
    t.drug_concept_id,
    c.concept_name,
    c.concept_code,
    COALESCE(CAST(t.{vn$start_datetime_var} AS DATE), t.{vn$start_date_var}) AS table_date,
    COALESCE(t.{vn$start_datetime_var}, CAST(t.{vn$start_date_var} AS TIMESTAMP)) AS table_datetime,
    COALESCE(CAST(t.{vn$end_datetime_var} AS DATE), t.{vn$end_date_var}) AS table_end_date,
    COALESCE(t.{vn$end_datetime_var}, CAST(t.{vn$end_date_var} AS TIMESTAMP)) AS table_end_datetime
    FROM @schema.drug_exposure t
    INNER JOIN @schema.concept c
      ON c.concept_id = t.drug_concept_id
    WHERE t.person_id IN (SELECT person_id FROM person_batch)
      AND {where_expression} )

    , drg AS (
      SELECT
          t.person_id
          ,t.icu_admission_datetime
          ,gs.time_in_icu
          {variables}
      FROM (
          SELECT
          adm.person_id,
          adm.icu_admission_datetime,
          i.drug_exposure_id,
          i.drug_concept_id,
          i.concept_name,
          i.concept_code,
          {window_start} AS drug_start,
          {window_end} AS drug_end
          FROM icu_admission_details_multiple_visits adm
          INNER JOIN drg_filtered i
          ON adm.person_id = i.person_id
        	AND (adm.visit_occurrence_id = i.visit_occurrence_id
        	      OR ((i.visit_occurrence_id IS NULL)
        	          AND (i.table_datetime >= adm.hospital_admission_datetime)
        	          AND (i.table_datetime < adm.hospital_discharge_datetime)))
                AND ({window_start} >= @first_window OR {window_end} >= @first_window)
                AND ({window_start} <= @last_window OR {window_end} <= @last_window)
      ) t
      {drug_join}
      GROUP BY
      t.person_id
      ,t.icu_admission_datetime
      ,gs.time_in_icu)

    SELECT * FROM drg;")

  drug_sql
}
