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
  # Units of measure join.
  units_of_measure_query <- units_of_measure_query(table_name)

  # Constructing main query
  with_query <-
    glue("
    , {variable_names$alias} as (
    SELECT
     t.person_id
    ,t.visit_occurrence_id
    ,t.visit_detail_id
  	,{window} as time_in_icu
     {variables}
    FROM icu_admission_details adm
    INNER JOIN @schema.{variable_names$db_table_name} t
    -- making sure the visits match up, and filtering by number of days in ICU
  	ON adm.person_id = t.person_id
  	AND adm.visit_occurrence_id = t.visit_occurrence_id
  	AND (adm.visit_detail_id = t.visit_detail_id OR adm.visit_detail_id IS NULL)
  	AND {window} >= @first_window
  	AND {window} <= @last_window
    {units_of_measure_query}
    -- For string searching by concept name if required
    -- Slightly odd alias, but using it to match drug table
    INNER JOIN @schema.concept t_w
    ON t_w.concept_id = t.{variable_names$concept_id_var}
    GROUP BY t.person_id
  	,t.visit_occurrence_id
  	,t.visit_detail_id
  	,{window} )
         ")
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
  string_search_expression = string_search_expression(concepts,
                                                      variable_names, "Drug")

  # Drug join
  drug_join <- translate_drug_join(dialect)

  drug_with_query <-
    glue("
    , drg as (
      --- Note, this query will double count overlaps.
      --- If a person has two versions of a single drug, with overalapping start and end dates,
      --- the drug will be double counted.
      SELECT
          t_w.person_id
          ,t_w.visit_occurrence_id
          ,t_w.visit_detail_id
          ,time_in_icu
          {variables}
      --- Filtering whole table for string matches so don't need to lateral join the whole thing
      FROM (
          SELECT
          adm.person_id
          ,adm.visit_occurrence_id
          ,adm.visit_detail_id
          ,t.drug_exposure_id
          ,t.drug_concept_id
          ,c.concept_name
          ,c.concept_code
          ,{window_start} as drug_start
          ,{window_end} as drug_end
          FROM icu_admission_details adm
          INNER JOIN @schema.drug_exposure t
          ON adm.person_id = t.person_id
          AND adm.visit_occurrence_id = t.visit_occurrence_id
          AND (adm.visit_detail_id = t.visit_detail_id OR adm.visit_detail_id IS NULL)
          AND ({window_start} >= @first_window OR {window_end} >= @first_window)
          AND ({window_start} <= @last_window OR {window_end} <= @last_window)
          INNER JOIN @schema.concept c
          ON c.concept_id = t.drug_concept_id
          WHERE {string_search_expression}
      ) t_w
      {drug_join}
      GROUP BY
      t_w.person_id
      ,t_w.visit_occurrence_id
      ,t_w.visit_detail_id
      ,time_in_icu
      )
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
  visit_occurrence_id <- gsub("placeholder", "visit_occurrence_id", prev_alias)
  visit_detail_id <- gsub("placeholder", "visit_detail_id", prev_alias)

  # The first table is just a from statement
  if(is.na(prev_alias)) {
    end_join_query = glue("FROM {variable_names$alias}")
  } else {
    end_join_query <-
      glue("
    FULL JOIN {variable_names$alias}
        ON COALESCE({person_id}) = {variable_names$alias}.person_id
       AND COALESCE({visit_occurrence_id}) = {variable_names$alias}.visit_occurrence_id
       AND (COALESCE({visit_detail_id}) = {variable_names$alias}.visit_detail_id OR
            COALESCE({visit_detail_id}) IS NULL)
       AND COALESCE({time_in_icu}) = {variable_names$alias}.time_in_icu
       AND {variable_names$alias}.time_in_icu >= @first_window
			 AND {variable_names$alias}.time_in_icu <= @last_window
         ")
  }
  end_join_query
}
