with_query <- function(concepts, table_name, variable_names,
                       window_start_point, cadence){
  # Constructs two CTEs per OMOP table (except drugs, handled separately):
  #   {alias}_filtered: filters the clinical table by person_batch and concept,
  #                     materialising a small intermediate result on Postgres 9.
  #   {alias}:          joins the filtered rows to admissions and aggregates.
  #
  # person_batch filtering uses IN (SELECT ...) rather than INNER JOIN.
  # On Postgres 9, INNER JOIN to a temp table pins the planner to the
  # person_id index. IN (subquery) lets the planner choose between the
  # person_id and concept_id indexes — typically 10-20x faster on tables
  # like measurement where the concept_id index is more selective.

  # Variable names, and return empty string if no concepts required
  concepts <- concepts %>%
    filter(table == table_name) %>%
    # Visit detail can only be queried for the emergency admission variable
    filter(table != "Visit Detail" |
             (table == "Visit Detail" & short_name == "emergency_admission")) %>%
    # Drugs are handled separately
    filter(table != "Drug")

  variable_names <- variable_names %>%
    filter(table == table_name)

  if (nrow(concepts) == 0){
    return("")
  }

  # Build datetime expressions.
  # start_datetime_var = the datetime column (e.g. measurement_datetime)
  # start_date_var     = the date column     (e.g. measurement_date)
  # Prefer the datetime column; fall back to the date column with a cast.
  datetime_expr <- glue(
    "COALESCE(t.{variable_names$start_datetime_var}, CAST(t.{variable_names$start_date_var} AS TIMESTAMP))")
  date_expr <- glue(
    "COALESCE(CAST(t.{variable_names$start_datetime_var} AS DATE), t.{variable_names$start_date_var})")

  # Windowing query. Visit detail window time is always 0, since it's the beginning of the admission.
  # Two versions:
  #   window               — uses raw column expressions, for reference
  #   window_from_filtered — uses the pre-computed table_date/table_datetime columns from
  #                          _filtered, avoiding redundant COALESCE/CAST in the aggregation CTE.
  if(table_name != "Visit Detail"){
    window <- window_query(window_start_point, datetime_expr, date_expr, cadence)
    window_from_filtered <- window_query(window_start_point, "t.table_datetime", "t.table_date", cadence)
  } else {
    window <- 0
    window_from_filtered <- 0
  }

  # Variable query
  variables <- variables_query(concepts,
                               variable_names$concept_id_var,
                               variable_names$id_var)

  # Where query
  where_expression <- where_clause(concepts, variable_names, table_name)

  # Units of measure join (only for tables with numeric values).
  units_of_measure_join <- units_of_measure_query(table_name)

  # Only join the concept table in _filtered if string search variables are
  # present for this table — concept_name/concept_code columns are only needed
  # in that case. For pure concept_id queries the join is unnecessary overhead.
  needs_concept_join <- any(concepts$omop_variable %in% c("concept_name", "concept_code"),
                            na.rm = TRUE)
  concept_join <- if (needs_concept_join) {
    glue("INNER JOIN @schema.concept c\n  ON c.concept_id = t.{variable_names$concept_id_var}")
  } else {
    ""
  }

  # Build explicit column list for _filtered instead of t.* to reduce
  # CTE materialization width. Only select columns the aggregation CTE needs.
  # NOTE: if you add a new omop_variable type that references a column not
  # listed here, you must add it to this list or the query will fail.
  has_numeric <- any(concepts$omop_variable == "value_as_number", na.rm = TRUE)
  has_value_as_concept <- any(concepts$omop_variable == "value_as_concept_id", na.rm = TRUE)
  additional_cols <- unique(concepts$additional_filter_variable_name[
    !is.na(concepts$additional_filter_variable_name)])

  filtered_cols <- c(
    "t.person_id",
    "t.visit_occurrence_id",
    glue("t.{variable_names$concept_id_var}"),
    glue("t.{variable_names$id_var}")
  )
  if (has_numeric) {
    filtered_cols <- c(filtered_cols, "t.value_as_number", "t.unit_concept_id")
  }
  if (has_value_as_concept) {
    filtered_cols <- c(filtered_cols, "t.value_as_concept_id")
  }
  if (length(additional_cols) > 0) {
    filtered_cols <- c(filtered_cols, glue("t.{additional_cols}"))
  }
  filtered_select <- glue_collapse(unique(filtered_cols), sep = ",\n      ")

  with_query <-
    glue("
    -- {table_name}: filter by person and concept to materialise a small intermediate result
    , {variable_names$alias}_filtered AS (
    SELECT
      {filtered_select},
      {date_expr} AS table_date,
      {datetime_expr} AS table_datetime
      {if (needs_concept_join) ', c.concept_name, c.concept_code' else ''}
    FROM @schema.{variable_names$db_table_name} t
    {concept_join}
    WHERE t.person_id IN (SELECT person_id FROM person_batch)
      AND ({where_expression}))

    -- {table_name}: join filtered rows to admissions and aggregate
    , {variable_names$alias} AS (
    SELECT
       t.person_id,
       adm.icu_admission_datetime,
       {window_from_filtered} AS time_in_icu
       {variables}
    FROM icu_admission_details_multiple_visits adm
    INNER JOIN {variable_names$alias}_filtered t
      ON adm.person_id = t.person_id
      AND (
         adm.visit_occurrence_id = t.visit_occurrence_id
         OR (t.visit_occurrence_id IS NULL
             AND t.table_datetime >= adm.hospital_admission_datetime
             AND t.table_datetime <  adm.hospital_discharge_datetime)
        )
    {units_of_measure_join}
    WHERE {window_from_filtered} >= @first_window
      AND {window_from_filtered} <= @last_window
    GROUP BY
      t.person_id,
      adm.icu_admission_datetime,
      {window_from_filtered}) ")
  with_query
}


drug_with_query <- function(concepts, variable_names,
                            window_start_point, cadence,
                            dialect){

  # The drug table needs generate_series / OUTER APPLY to expand date ranges
  # into individual time windows. It keeps a _filtered CTE because the lateral
  # join needs a pre-filtered set to expand.

  concepts <- concepts %>%
    filter(table == "Drug")
  variable_names <- variable_names %>%
    filter(table == "Drug")

  if (nrow(concepts) == 0){
    return("")
  }

  # Window query
  window_start <- window_query(window_start_point,
                               "i.table_datetime",
                               "i.table_date",
                               cadence)
  window_end <- window_query(window_start_point,
                             "i.table_end_datetime",
                             "i.table_end_date",
                             cadence)

  # Variables query
  variables <- variables_query(concepts,
                               variable_names$concept_id_var,
                               variable_names$id_var)
  where_expression <- where_clause(concepts, variable_names, "Drug")
  drug_join <- translate_drug_join(dialect)

  drug_with_query <-
    glue("
    -- Drug: filter by person and concept
    -- Uses IN (subquery) instead of JOIN to person_batch so the planner
    -- can choose the concept_id index on large tables (Postgres 9 quirk).
    , drg_filtered AS (
    SELECT
    t.person_id,
    t.visit_occurrence_id,
    t.drug_exposure_id,
    t.drug_concept_id,
    c.concept_name,
    c.concept_code,
    COALESCE(CAST(t.{variable_names$start_datetime_var} AS DATE), t.{variable_names$start_date_var}) AS table_date,
    COALESCE(t.{variable_names$start_datetime_var}, CAST(t.{variable_names$start_date_var} AS TIMESTAMP)) AS table_datetime,
    COALESCE(CAST(t.{variable_names$end_datetime_var} AS DATE), t.{variable_names$end_date_var}) AS table_end_date,
    COALESCE(t.{variable_names$end_datetime_var}, CAST(t.{variable_names$end_date_var} AS TIMESTAMP)) AS table_end_datetime
    FROM @schema.drug_exposure t
    INNER JOIN @schema.concept c
      ON c.concept_id = t.drug_concept_id
    WHERE t.person_id IN (SELECT person_id FROM person_batch)
      AND {where_expression} )

    -- Drug: join to admissions, expand date ranges, aggregate
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
      ,gs.time_in_icu)")
  drug_with_query
}


#' Build a UNION spine of all (person_id, icu_admission_datetime, time_in_icu)
#' from every table CTE. Replaces the old FULL JOIN chain with COALESCE keys.
#' UNION (not UNION ALL) deduplicates, so a patient-day present in multiple
#' tables appears once in the spine — no need for DISTINCT in each branch.
#'
#' @param aliases Character vector of CTE alias names (e.g. c("m", "o", "drg"))
#' @return A SQL string defining the spine CTE.
spine_query <- function(aliases) {
  union_parts <- glue(
    "SELECT person_id, icu_admission_datetime, time_in_icu FROM {aliases}")
  glue(", spine AS (\n    ",
       glue_collapse(union_parts, sep = "\n    UNION\n    "),
       "\n  )")
}


#' Build LEFT JOINs from the spine to each table CTE.
#' Each join is on plain columns (no COALESCE), so the DB can use indexes.
#'
#' @param aliases Character vector of CTE alias names.
#' @return A SQL string with FROM spine + LEFT JOIN clauses.
spine_join_query <- function(aliases) {
  joins <- glue("
    LEFT JOIN {aliases}
      ON spine.person_id = {aliases}.person_id
      AND spine.icu_admission_datetime = {aliases}.icu_admission_datetime
      AND spine.time_in_icu = {aliases}.time_in_icu")
  glue("FROM spine\n", glue_collapse(joins, sep = "\n"))
}
