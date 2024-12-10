with_query <- function(concepts, table_name, variable_names,
                           window_start_point, cadence){

  # Variable names, and return empty string if no concepts required
  concepts <- concepts %>%
    filter(table == table_name) %>%
    # Visit detail can only be queried for the emergency admission variable
    filter(table != "Visit Detail" |
             (table == "Visit Detail" & short_name == "emergency_admission"))

  variable_names <- variable_names %>%
    filter(table == table_name)

  if (nrow(concepts == 0)){
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
  # Units of measure
  units_of_measure_query <- units_of_measure_query(table_name)

  # Constructing main query
  with_query <-
    glue("
    , WITH {alias} as (
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

end_join_query <- function(table_name, variable_names, prev_alias){

  # The first table in the combined join doesn't have a previous time variable to join to.
  # But all the others have to.
  if(!is.na(prev_alias)) {
    time_join = glue("AND {prev_alias}.time_in_icu = {alias}.time_in_icu")
  } else {
    time_join = ""
  }

  end_join_query <-
    glue("
    OUTER JOIN {alias}
        ON adm.person_id = {alias}.person_id
       AND adm.visit_occurrence_id = {alias}.visit_occurrence_id
       AND (adm.visit_detail_id = {alias}.visit_detail_id OR adm.visit_detail_id IS NULL)
       {time_join}
       AND {alias}.time_in_icu >= @first_window
			 AND {alias}.time_in_icu < @last_window
         ")
}
