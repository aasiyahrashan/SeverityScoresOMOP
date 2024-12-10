with_statement <- function(table_name, variable_names){

  # Variable names
  variable_names <- variable_names %>%
    filter(table == table_name)

  # Windowing query

  # Variable query

  # Units of measure - empty if the table isn't the measurement one.
  units_of_measure_query <- units_of_measure_query(table_name)

  # Constructing main query
  with_query <-
    glue("
    , WITH {table_name} as (
     t.person_id
    ,t.visit_occurrence_id
    ,t.visit_detail_id
  	,{window} as time_in_icu
    {variables}
    INNER JOIN @schema.{db_table_name}
    -- making sure the visits match up, and filtering by number of days in ICU
  	ON adm.person_id = t.person_id
  	AND adm.visit_occurrence_id = t.visit_occurrence_id
  	AND (adm.visit_detail_id = t.visit_detail_id OR adm.visit_detail_id IS NULL)
  	AND {window} >= '@first_window'
  	AND {window} < '@last_window'
    {units_of_measure_query}
    GROUP BY t.person_id
  	,t.visit_occurrence_id
  	,t.visit_detail_id
  	,{window}
         ")
}
