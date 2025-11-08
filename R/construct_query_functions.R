#' Create a connection object to on OMOP compliant database.
#' @param driver driver eg. "SQL Server", "PostgreSQL", etc.
#' @param host host/server name
#' @param dbname name of database in server
#' @param port database port
#' @param user username
#' @param password password
#' @importFrom DBI dbConnect
#' @importFrom RPostgres Postgres
#' @importFrom odbc odbc
#' @importFrom SqlRender translate render
#' @export
omop_connect <-
  function(driver, host, dbname, port, user, password) {
    if (driver == "PostgreSQL") {
      dbConnect(
        Postgres(),
        host = host,
        dbname = dbname,
        port = port,
        user = user ,
        password = password
      )
    } else {
      dbConnect(
        odbc(),
        driver = driver,
        server = host,
        database = dbname,
        uid = user,
        pwd = password
      )
    }
  }

# Builds the 'time_in_icu' windowing query based on variable names in each table.
window_query <- function(window_start_point, time_variable,
                         date_variable, cadence){

  # Checking arguments
  if (!(window_start_point %in% c("calendar_date", "icu_admission_time"))) {
    stop("Error: window_start_point must be either 'calendar_date' or 'icu_admission_time'")
  }

  if (window_start_point == "calendar_date" & cadence != 24) {
    stop("Error: Since window_start_point is set to 'calendar_date', cadence must be 24.
         Either edit the window_start_point to 'icu_admission_time', or edit the cadence to 24.")
  }

  if(!cadence > 0){
    stop("cadence must be a number > 0'")
  }

  # Constructing query
  ifelse(
    window_start_point == "calendar_date",
    # Returns one row per day based on calendar date rather than admission time.
    glue(" DATEDIFF(dd, adm.icu_admission_datetime, {date_variable})"),
    # Uses admission date as starting point, returns rows based on cadence argument.
    glue(" FLOOR(DATEDIFF(MINUTE, adm.icu_admission_datetime, {time_variable} / ({cadence} * 60))"))
}

# Builds the age query
age_query <- function(age_method, dialect) {

  if (!(age_method %in% c("dob", "year_only"))) {
    stop("Error: age_method must be either 'dob' or 'year_only'")
  }

  age_query <- ifelse(
    age_method == "dob",
    " DATEDIFF(yyyy,
			  COALESCE(birth_datetime, DATEFROMPARTS(year_of_birth, COALESCE(month_of_birth, '06'),
          COALESCE(day_of_birth, '01'))), icu_admission_datetime) as age",
    " YEAR(icu_admission_datetime) - year_of_birth AS age")
}

# Builds the 'WHERE' query to make sure we only filter by required variables SQL
# function is only called for patients and timepoints with the drugs of interest.
where_clause <- function(concepts,
                                     variable_names,
                                     table_name) {

  # Finding variables which need string matches
  concepts <- concepts %>%
    filter(table == table_name)
  variable_names <- variable_names %>%
    filter(table == table_name)

  # Getting dataframes for each variable.
  concept_ids_required <- concepts %>%
    filter(!omop_variable %in% c("concept_name", "concept_code", "ancestor_concept_id") |
             is.na(omop_variable))
  strings_required <- concepts %>%
    filter(omop_variable %in% c("concept_name", "concept_code"))
  ancestor_concept_ids_required <- concepts %>%
    filter(omop_variable == "ancestor_concept_id")

  # Need a section for concept ID queries
  concept_id_expression <-
    concept_ids_required %>%
    reframe(
      concept_id = glue("{variable_names$concept_id_var} IN (",
                        glue_collapse(unique(concept_id), sep = ", "),
                        ")")) %>%
    pull(concept_id)

  # This collapsed list is used for string queries
  string_search_expression <-
    strings_required %>%
    distinct(omop_variable, concept_id) %>%
    reframe(
      concept_id =
        glue("LOWER({omop_variable}) LIKE '% ",
             glue_collapse(tolower(concept_id),
                                 sep = "%' OR LOWER({omop_variable}) LIKE '%"),
             "%'")) %>%
    pull(concept_id)

  # This list is ancestor concept.
  ancestor_expression <-
    ancestor_concept_ids_required %>%
    distinct(omop_variable, concept_id) %>%
    reframe(
      concept_id =
        glue("{variable_names$concept_id_var} IN (SELECT descendant_concept_id FROM
        @schema.concept_ancestor WHERE
        ancestor_concept_id IN (",
             glue_collapse(unique(concept_id), sep = ", "),
             "))"))


  # Joining based on rows filled in.
  expressions <- c()
  if (nrow(concept_ids_required) > 0) expressions <- c(expressions, concept_id_expression)
  if (nrow(strings_required) > 0) expressions <- c(expressions, string_search_expression)
  if (nrow(ancestor_concept_ids_required) > 0) expressions <- c(expressions, ancestor_expression)

  # Combine with OR, or use 'false' if none are present
  # If no concepts, I want no row returned. So I make the condition say 'where false'.
  combined_expression <- if (length(expressions) > 0) {
   paste0("(", paste(expressions, collapse = " OR "), ")")
  } else {
    "false"
  }
  combined_expression
}

# Builds the CASE WHEN 'variable required' query for each table
variables_query <- function(concepts,
                             concept_id_var,
                             table_id_var = ""){

  variables_required = ""

  # Numeric variables
  numeric_concepts <-
    concepts %>%
    filter(omop_variable == "value_as_number") %>%
    #### GCS can sometimes be stored as a concept ID instead
    #### of a number. These need a separate query.
    filter(!(short_name %in% c("gcs_eye", "gcs_motor", "gcs_verbal") &
               omop_variable == "value_as_concept_id")) %>%
    # It's possible for multiple concept IDs to represent the same variable, so grouping here.
    group_by(short_name, omop_variable, additional_filter_variable_name) %>%
    summarise(
      # This is slightly odd, but just makes sure we don't duplicate concept
      # IDs in cases where we're selecting specific values
      # The query returns the number of rows matching the concept IDs provided.
      concept_id = glue_collapse(unique(concept_id), sep = ", "),
      additional_filter_value = glue(
        "'",
        glue_collapse(additional_filter_value, sep = "', '"),
        "'"
      )
    ) %>%
    mutate(
      additional_filter_query =
        if_else(!is.na(additional_filter_variable_name),
                glue("AND {additional_filter_variable_name}
              IN ({additional_filter_value})"), "", ""),
      max_query = glue(
        ", MAX(CASE WHEN {concept_id_var} IN ({concept_id})
           {additional_filter_query}
                    THEN {omop_variable}
               END) AS max_{short_name}"
      ),
      min_query = glue(
        ", MIN(CASE WHEN {concept_id_var} IN ({concept_id})
           {additional_filter_query}
                    THEN {omop_variable}
               END) AS min_{short_name}"
      ),
      unit_query = glue(
        ", MIN(CASE WHEN {concept_id_var} IN ({concept_id})
           {additional_filter_query}
                    THEN c_unit.concept_name
               END) AS unit_{short_name}"
      ))

  # Non-numeric variables if concept ID provided. Returns counts.
  non_numeric_concepts <-
    concepts %>%
    filter((omop_variable == "value_as_concept_id" |
                is.na(omop_variable))) %>%
    # It's possible for multiple concept IDs to represent the same variable, so grouping here.
    group_by(short_name, omop_variable, additional_filter_variable_name) %>%
    summarise(
      # This is slightly odd, but just makes sure we don't duplicate concept
      # IDs in cases where we're selecting specific values
      # The query returns the number of rows matching the concept IDs provided.
      concept_id = glue_collapse(unique(concept_id), sep = ", "),
      concept_id_value = glue_collapse(concept_id_value, sep = ", "),
      additional_filter_value = glue(
        "'",
        glue_collapse(additional_filter_value, sep = "', '"),
        "'"
      )
    ) %>%
    # Building the query in separate elements, depending on which conditions are filled
    mutate(
      value_as_concept_id_query = if_else(
        omop_variable == "value_as_concept_id",
        glue("AND value_as_concept_id
              IN ({concept_id_value})"), "", ""),
      additional_filter_query =
        if_else(!is.na(additional_filter_variable_name),
                glue("AND {additional_filter_variable_name}
              IN ({additional_filter_value})"), "", ""),
      count_query = glue(
        ", COUNT ( CASE WHEN {concept_id_var} IN ({concept_id})
           {value_as_concept_id_query}
           {additional_filter_query}
           THEN {table_id_var}
           END ) AS count_{short_name}"))

  # Non-numeric variables if no concept ID provided.
  # Does a string search on concept name.
  # Returns counts.
  non_numeric_string_search_concepts <-
    concepts %>%
    filter(omop_variable %in% c("concept_name", "concept_code")) %>%
    # It's possible for multiple strings to represent the same variable, so grouping here.
    group_by(short_name, omop_variable, additional_filter_variable_name) %>%
    summarise(
      # This is slightly odd, but just makes sure we don't duplicate concept
      # IDs in cases where we're selecting specific values
      # The query returns the number of rows matching the concept IDs provided.
      concept_id = glue_collapse(tolower(unique(concept_id)),
                                      sep = glue("%' OR LOWER(t_w.{omop_variable}) LIKE '%")),
      additional_filter_value = glue(
        "'",
        glue_collapse(additional_filter_value, sep = "', '"),
        "'"
      )
    ) %>%
    # Building the query in separate elements, depending on which conditions are filled
    mutate(
      additional_filter_query =
        if_else(!is.na(additional_filter_variable_name),
                glue("AND {additional_filter_variable_name}
              IN ({additional_filter_value})"), "", ""),
      count_query = glue(
        ", COUNT ( CASE WHEN LOWER(t_w.{omop_variable}) LIKE '%{concept_id}%'
           {additional_filter_query}
           THEN {table_id_var}
           END ) AS count_{short_name}"))

  # Ancestor concepts
  ancestor_concepts <-
    concepts %>%
    filter(omop_variable %in% c("ancestor_concept_id")) %>%
    # It's possible for multiple concept IDs to represent the same variable, so grouping here.
    group_by(short_name, omop_variable, additional_filter_variable_name) %>%
    summarise(
      # This is slightly odd, but just makes sure we don't duplicate concept
      # IDs in cases where we're selecting specific values
      # The query returns the number of rows matching the concept IDs provided.
      concept_id = glue_collapse(unique(concept_id), sep = ", "),
      additional_filter_value = glue(
        "'",
        glue_collapse(additional_filter_value, sep = "', '"),
        "'"
      )
    ) %>%
    mutate(
      additional_filter_query =
          if_else(!is.na(additional_filter_variable_name),
                  glue("AND {additional_filter_variable_name}
              IN ({additional_filter_value})"), "", ""),
        count_query = glue(
      ", COUNT ( CASE WHEN {concept_id_var} in (SELECT descendant_concept_id FROM
            @schema.concept_ancestor WHERE
            ancestor_concept_id IN ({concept_id}))
            {additional_filter_query}
            THEN {table_id_var} END) AS count_{short_name}"))


  # Collapsing the queries into strings.
  variables_required <-
    glue(glue_collapse(c(numeric_concepts$max_query,
                         numeric_concepts$min_query,
                         numeric_concepts$unit_query,
                         non_numeric_concepts$count_query,
                         non_numeric_string_search_concepts$count_query,
                         ancestor_concepts$count_query),
                       sep = "\n"), "\n")

  # Return query
  variables_required
}

# Translates the left lateral join between postgres and sql server. for the drug table.
# Needs to be done in R, because the SQLRender package doesn't support the more complicated joins.
translate_drug_join <- function(dialect){

  if (dialect == "postgresql"){
    drug_join <- glue(
    "LEFT JOIN LATERAL
      --- For each row in the table, creating
      generate_series(
       t_w.drug_start
      ,t_w.drug_end
      --- The 'on true' condition just means that every row in the drug table gets joined to
      --- the corresponding time_in_icu rows created by generate_series.
      ) AS gs(time_in_icu) ON TRUE")
  }

  if (dialect == "sql server"){
    drug_join <- glue(
    "OUTER APPLY
      --- For each row in the table, creating
      generate_series(
      t_w.drug_start
      , t_w.drug_end
      --- Every row in the drug table gets joined to
      --- the corresponding time_in_icu rows created by generate_series.
      ) AS time_in_icu")

  }
  drug_join
}

# Create join to the concept table to get unit of measure name
units_of_measure_query <- function(table_name){
  # Only applies to tables with numeric values
  if(table_name %in% c("Measurement",
                       "Observation")){
    units_of_measure_query <- glue(
      "
      -- getting unit of measure for numeric variables.
       LEFT JOIN @schema.concept c_unit ON t.unit_concept_id = c_unit.concept_id
  	   AND t.unit_concept_id IS NOT NULL")
  } else {
    units_of_measure_query <- ""
  }
  units_of_measure_query
}

# Create list of all physiology variables which will be returned,
# for the select statement in the main sql query.
all_required_variables_query <- function(concepts){
  all_required_variables <- concepts %>%
    mutate(short_name =
             case_when(omop_variable %in%
                         c("value_as_concept_id",
                           "concept_name", "concept_code",
                           "ancestor_concept_id") |
                         is.na(omop_variable) ~
                         glue("count_{short_name}"),
                       omop_variable == "value_as_number" ~
                         glue("min_{short_name}, max_{short_name},
                              unit_{short_name}"))) %>%
    distinct(short_name)

  glue(",", glue_collapse(all_required_variables$short_name, sep = ", "))

}

# Given a vector of table aliases, returns them
# in a single comma separated string with a variable name pasted on.
all_id_vars <- function(alias, string){
  all_time_in_icu <- glue_collapse(
    glue("{alias}.{string}"),
    sep = ", ")
}
