# =============================================================================
# construct_with_statement.R
#
# Builds standalone SQL queries for each OMOP clinical table. Each query
# is self-contained (includes the pasted visits CTE preamble) and returns
# physiology data for one table. R merges results after execution.
#
# Why separate queries instead of one big CTE chain:
#   - Postgres 9 CTEs are optimisation fences — chaining them causes the
#     planner to hang.
#   - Each query is simple, focused, and independently debuggable.
#   - R merging is instant on aggregated data, so there's no penalty.
# =============================================================================


#' Build a _filtered CTE for a non-Drug OMOP table.
#'
#' Materialises a small intermediate result by filtering the clinical table
#' by person_batch (via IN subquery) and concept IDs.
#'
#' @param table_concepts Concepts dataframe, pre-filtered to this table.
#' @param table_name OMOP table name.
#' @param variable_names Variable names dataframe (full, will be filtered).
#'
#' @return A SQL CTE fragment: ", {alias}_filtered AS (...)".
#' @importFrom glue glue glue_collapse
#' @keywords internal
filtered_cte <- function(table_concepts, table_name, variable_names) {

  vn <- variable_names[variable_names$table == table_name, ]

  # Visit Detail has no datetime columns (NA in variable_names.csv).
  # readr::read_delim converts empty CSV cells to NA, and nzchar(NA)
  # returns TRUE, so we must check for NA explicitly.
  has_dates <- !is.na(vn$start_datetime_var) || !is.na(vn$start_date_var)

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

  # Build minimal column list
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

  glue("
    , {vn$alias}_filtered AS (
    SELECT
      {select_list}{date_select}
      {concept_cols}
    FROM @schema.{vn$db_table_name} t
    {concept_join}
    WHERE t.person_id IN (SELECT person_id FROM person_batch)
      AND ({where_expr}))")
}


#' Build a complete long-format query for numeric variables in one table.
#'
#' Returns rows: (person_id, icu_admission_datetime, time_in_icu,
#' concept_id, min_val, max_val, unit_name [, filter columns]).
#'
#' @param table_concepts Concepts for this table (all types; filtered internally).
#' @param table_name OMOP table name.
#' @param variable_names Variable names dataframe.
#' @param pasted_visits_sql Pre-rendered visit identification SQL.
#' @param window_start_point Windowing mode.
#' @param cadence Cadence in hours.
#'
#' @return A complete SQL query string, or "" if no numeric variables.
#' @importFrom glue glue
#' @keywords internal
build_long_query <- function(table_concepts, table_name, variable_names,
                             pasted_visits_sql, window_start_point, cadence) {

  numeric <- table_concepts[table_concepts$omop_variable == "value_as_number" &
                              !is.na(table_concepts$omop_variable), ]
  if (nrow(numeric) == 0) return("")

  vn <- variable_names[variable_names$table == table_name, ]

  window_expr <- if (table_name != "Visit Detail") {
    window_query(window_start_point, "t.table_datetime", "t.table_date", cadence)
  } else "0"

  # Additional filter columns (for disambiguation during R pivot)
  filter_cols <- unique(numeric$additional_filter_variable_name[
    !is.na(numeric$additional_filter_variable_name)])
  extra_select <- if (length(filter_cols) > 0) {
    paste0(",\n      t.", filter_cols, collapse = "")
  } else ""
  extra_group <- if (length(filter_cols) > 0) {
    paste0(",\n      t.", filter_cols, collapse = "")
  } else ""

  filt <- filtered_cte(numeric, table_name, variable_names)

  long_cte <- glue("
    , {vn$alias}_long AS (
    SELECT
      t.person_id,
      adm.icu_admission_datetime,
      {window_expr} AS time_in_icu,
      t.{vn$concept_id_var} AS concept_id,
      MIN(t.value_as_number) AS min_val,
      MAX(t.value_as_number) AS max_val,
      MIN(c_unit.concept_name) AS unit_name{extra_select}
    FROM icu_admission_details_multiple_visits adm
    INNER JOIN {vn$alias}_filtered t
      ON adm.person_id = t.person_id
      AND (adm.visit_occurrence_id = t.visit_occurrence_id
           OR (t.visit_occurrence_id IS NULL
               AND t.table_datetime >= adm.hospital_admission_datetime
               AND t.table_datetime <  adm.hospital_discharge_datetime))
    LEFT JOIN @schema.concept c_unit
      ON t.unit_concept_id = c_unit.concept_id
      AND t.unit_concept_id IS NOT NULL
    WHERE {window_expr} >= @first_window
      AND {window_expr} <= @last_window
      AND t.value_as_number IS NOT NULL
    GROUP BY
      t.person_id, adm.icu_admission_datetime, {window_expr},
      t.{vn$concept_id_var}{extra_group})")

  glue("{pasted_visits_sql}\n{filt}\n{long_cte}\n",
       "SELECT * FROM {vn$alias}_long;")
}


#' Build a complete count query for non-numeric variables in one table.
#'
#' Returns rows: (person_id, icu_admission_datetime, time_in_icu,
#' count_{short_name_1}, count_{short_name_2}, ...).
#'
#' @param table_concepts Concepts for this table (all types; filtered internally).
#' @param table_name OMOP table name.
#' @param variable_names Variable names dataframe.
#' @param pasted_visits_sql Pre-rendered visit identification SQL.
#' @param window_start_point Windowing mode.
#' @param cadence Cadence in hours.
#'
#' @return A complete SQL query string, or "" if no count variables.
#' @importFrom glue glue
#' @keywords internal
build_count_query <- function(table_concepts, table_name, variable_names,
                              pasted_visits_sql, window_start_point, cadence) {

  counts <- table_concepts[
    table_concepts$omop_variable != "value_as_number" |
      is.na(table_concepts$omop_variable), ]
  if (nrow(counts) == 0) return("")

  vn <- variable_names[variable_names$table == table_name, ]

  window_expr <- if (table_name != "Visit Detail") {
    window_query(window_start_point, "t.table_datetime", "t.table_date", cadence)
  } else "0"

  variables <- variables_query(counts, vn$concept_id_var, vn$id_var)
  filt <- filtered_cte(counts, table_name, variable_names)

  # The join condition differs for Visit Detail (no datetime columns)
  join_condition <- if (table_name == "Visit Detail") {
    # Visit Detail rows join by visit_occurrence_id only
    glue("ON adm.person_id = t.person_id
      AND adm.visit_occurrence_id = t.visit_occurrence_id")
  } else {
    glue("ON adm.person_id = t.person_id
      AND (adm.visit_occurrence_id = t.visit_occurrence_id
           OR (t.visit_occurrence_id IS NULL
               AND t.table_datetime >= adm.hospital_admission_datetime
               AND t.table_datetime <  adm.hospital_discharge_datetime))")
  }

  count_cte <- glue("
    , {vn$alias}_counts AS (
    SELECT
       t.person_id,
       adm.icu_admission_datetime,
       {window_expr} AS time_in_icu
       {variables}
    FROM icu_admission_details_multiple_visits adm
    INNER JOIN {vn$alias}_filtered t
      {join_condition}
    WHERE {window_expr} >= @first_window
      AND {window_expr} <= @last_window
    GROUP BY
      t.person_id, adm.icu_admission_datetime, {window_expr})")

  glue("{pasted_visits_sql}\n{filt}\n{count_cte}\n",
       "SELECT * FROM {vn$alias}_counts;")
}


#' Build the Drug table query.
#'
#' Uses generate_series (Postgres) / OUTER APPLY (SQL Server) to expand
#' drug date ranges into individual time windows.
#'
#' @param concepts Full concepts dataframe.
#' @param variable_names Variable names dataframe.
#' @param pasted_visits_sql Pre-rendered visit identification SQL.
#' @param window_start_point Windowing mode.
#' @param cadence Cadence in hours.
#' @param dialect SQL dialect.
#'
#' @return A complete SQL query string, or "" if no Drug concepts.
#' @importFrom glue glue
#' @keywords internal
build_drug_query <- function(concepts, variable_names, pasted_visits_sql,
                             window_start_point, cadence, dialect) {

  drug <- concepts[concepts$table == "Drug", ]
  if (nrow(drug) == 0) return("")

  vn <- variable_names[variable_names$table == "Drug", ]

  ws <- window_query(window_start_point, "i.table_datetime", "i.table_date", cadence)
  we <- window_query(window_start_point, "i.table_end_datetime", "i.table_end_date", cadence)

  variables <- variables_query(drug, vn$concept_id_var, vn$id_var)
  where_expr <- where_clause(drug, variable_names, "Drug")
  drug_join <- translate_drug_join(dialect)

  glue("
    {pasted_visits_sql}

    , drg_filtered AS (
    SELECT
      t.person_id, t.visit_occurrence_id, t.drug_exposure_id, t.drug_concept_id,
      c.concept_name, c.concept_code,
      COALESCE(CAST(t.{vn$start_datetime_var} AS DATE), t.{vn$start_date_var}) AS table_date,
      COALESCE(t.{vn$start_datetime_var}, CAST(t.{vn$start_date_var} AS TIMESTAMP)) AS table_datetime,
      COALESCE(CAST(t.{vn$end_datetime_var} AS DATE), t.{vn$end_date_var}) AS table_end_date,
      COALESCE(t.{vn$end_datetime_var}, CAST(t.{vn$end_date_var} AS TIMESTAMP)) AS table_end_datetime
    FROM @schema.drug_exposure t
    INNER JOIN @schema.concept c ON c.concept_id = t.drug_concept_id
    WHERE t.person_id IN (SELECT person_id FROM person_batch)
      AND {where_expr})

    , drg AS (
      SELECT t.person_id, t.icu_admission_datetime, gs.time_in_icu
          {variables}
      FROM (
          SELECT adm.person_id, adm.icu_admission_datetime,
                 i.drug_exposure_id, i.drug_concept_id,
                 i.concept_name, i.concept_code,
                 {ws} AS drug_start, {we} AS drug_end
          FROM icu_admission_details_multiple_visits adm
          INNER JOIN drg_filtered i
            ON adm.person_id = i.person_id
            AND (adm.visit_occurrence_id = i.visit_occurrence_id
                 OR (i.visit_occurrence_id IS NULL
                     AND i.table_datetime >= adm.hospital_admission_datetime
                     AND i.table_datetime <  adm.hospital_discharge_datetime))
            AND ({ws} >= @first_window OR {we} >= @first_window)
            AND ({ws} <= @last_window  OR {we} <= @last_window)
      ) t
      {drug_join}
      GROUP BY t.person_id, t.icu_admission_datetime, gs.time_in_icu)

    SELECT * FROM drg;")
}
