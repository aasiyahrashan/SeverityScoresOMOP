with_query <- function(concepts, table_name, variable_names,
                           window_start_point, cadence){
  # Constructs subqueries for every table except the drug one.

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

  # Windowing query. Visit detail window time is always 0, since it's the beginning of the admission
  if(table_name != "Visit Detail"){
    window <- window_query(window_start_point,
                           variable_names$start_datetime_var,
                           variable_names$start_date_var,
                           cadence)
  } else {
    window <- 0
  }

  # Variable query
  variables <- variables_query(concepts,
                               variable_names$concept_id_var,
                               variable_names$id_var)

  # Where query
  where_clause = drugs_where_clause(concepts,
                                          variable_names, table_name)

  # Units of measure join.
  units_of_measure_query <- units_of_measure_query(table_name)

  # Constructing main query
  with_query <-
    glue("
    -- filter by person and concept ID to reduce table size
    CREATE TEMP TABLE {variable_names$alias}_filtered as
      SELECT
      *,
        COALESCE({variable_names$start_datetime_var}::date, {variable_names$start_date_var}) AS table_date,
        COALESCE({variable_names$start_datetime_var}, {variable_names$start_date_var}::timestamp) AS table_datetime
      FROM @schema.{variable_names$db_table_name}
      WHERE {where_clause}
        AND person_id IN (@person_ids);


    --- Filter by timestamp and create one row per time
    CREATE TEMP TABLE {variable_names$alias} as
    SELECT
       t.person_id,
       adm.icu_admission_datetime,
  	   ,{window} as time_in_icu
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
    {units_of_measure_query}
    -- For string searching by concept name if required
    -- Slightly odd alias, but using it to match drug table
    INNER JOIN @schema.concept t_w
    ON t_w.concept_id = t.{variable_names$concept_id_var}
    WHERE {window} >= @first_window
  	AND {window} <= @last_window
  	GROUP BY
    t.person_id,
    adm.icu_admission_datetime,
    {window} ;

  	DROP TABLE {variable_names$alias}_filtered;")
  with_query
}

drug_with_query <- function(concepts, variable_names,
                            window_start_point, cadence,
                            dialect){

  # The drug table query is completely different because it needs to account for both
  # start and end times.

  # Getting data for the drug table only
  concepts <- concepts %>%
    filter(table == "Drug")
  variable_names <- variable_names %>%
    filter(table == "Drug")

  if (nrow(concepts) == 0){
    return("")
  }

  # Window query
  window_start <- window_query(window_start_point,
                               variable_names$start_datetime_var,
                               variable_names$start_date_var,
                               cadence)
  window_end <- window_query(window_start_point,
                             variable_names$end_datetime_var,
                             variable_names$end_date_var,
                             cadence)

  # Variables query
  variables <- variables_query(concepts,
                               variable_names$concept_id_var,
                               variable_names$id_var)
  where_clause = where_clause(concepts, variable_names, "Drug")

  # Drug join
  drug_join <- translate_drug_join(dialect)

  drug_with_query <-
    glue("
    CREATE TEMP TABLE drg_non_aggregated AS
    SELECT *
         , {window_start} AS drug_start
         , {window_end} AS drug_end
    FROM icu_admission_details_multiple_visits adm
    INNER JOIN @schema.drug_exposure t
      ON adm.person_id = t.person_id
      AND (adm.visit_occurrence_id = t.visit_occurrence_id
           OR ((t.visit_occurrence_id IS NULL)
               AND (COALESCE(t.drug_exposure_start_datetime, t.drug_exposure_start_date) >= adm.hospital_admission_datetime)
               AND (COALESCE(t.drug_exposure_start_datetime, t.drug_exposure_start_date) < adm.hospital_discharge_datetime)))
    INNER JOIN @schema.concept c
      ON c.concept_id = t.drug_concept_id
    WHERE {where_clause}
      AND t.person_id IN (@person_ids)
      AND ({window_start} >= @first_window OR {window_end} >= @first_window)
      AND ({window_start} <= @last_window OR {window_end} <= @last_window);

      CREATE TEMP TABLE drg AS
      SELECT t_w.person_id
     ,t_w.icu_admission_datetime
     ,time_in_icu
     {variables}
     FROM (
    SELECT *
    FROM drg_non_aggregated
      ) t_w
      {drug_join}
      GROUP BY
      t_w.person_id
      ,t_w.icu_admission_datetime
      ,time_in_icu;
      DROP TABLE drg_non_aggregated;
         ")

  drug_with_query
}
end_join_query <- function(table_name, variable_names, prev_alias){
  # Constructs the segment that joins each 'with' subquery to other
  # subqueries run before it.

  # Reading variable names
  variable_names <- variable_names %>%
    filter(table == table_name)

  # Getting strings for coalesce
  time_in_icu <- gsub("placeholder", "time_in_icu", prev_alias)
  person_id <- gsub("placeholder", "person_id", prev_alias)
  icu_admission_datetime <- gsub("placeholder", "icu_admission_datetime", prev_alias)

  # The first table is just a from statement
  if(is.na(prev_alias)) {
    end_join_query = glue("FROM {variable_names$alias}")
  } else {
    end_join_query <-
      glue("
    FULL JOIN {variable_names$alias}
        ON COALESCE({person_id}) = {variable_names$alias}.person_id
       AND COALESCE({icu_admission_datetime}) = {variable_names$alias}.icu_admission_datetime
       AND COALESCE({time_in_icu}) = {variable_names$alias}.time_in_icu
       AND {variable_names$alias}.time_in_icu >= @first_window
			 AND {variable_names$alias}.time_in_icu <= @last_window
         ")
  }
  end_join_query
}
