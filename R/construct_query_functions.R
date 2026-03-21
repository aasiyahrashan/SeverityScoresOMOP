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

  ifelse(
    window_start_point == "calendar_date",
    glue(" DATEDIFF(dd, adm.icu_admission_datetime, {date_variable})"),
    glue(" FLOOR(DATEDIFF(MINUTE, adm.icu_admission_datetime, {time_variable}) / ({cadence} * 60))"))
}

# Builds the age query
age_query <- function(age_method) {

  if (!(age_method %in% c("dob", "year_only"))) {
    stop("Error: age_method must be either 'dob' or 'year_only'")
  }

  ifelse(
    age_method == "dob",
    " DATEDIFF(yyyy,
			  COALESCE(birth_datetime, DATEFROMPARTS(year_of_birth, COALESCE(month_of_birth, '06'),
          COALESCE(day_of_birth, '01'))), icu_admission_datetime) as age",
    " YEAR(icu_admission_datetime) - year_of_birth AS age")
}

# Builds the 'WHERE' clause: concept ID, string search, and ancestor concept filters.
where_clause <- function(concepts,
                         variable_names,
                         table_name) {

  concepts <- concepts %>% filter(table == table_name)
  variable_names <- variable_names %>% filter(table == table_name)

  concept_ids_required <- concepts %>%
    filter(!omop_variable %in% c("concept_name", "concept_code", "ancestor_concept_id") |
             is.na(omop_variable))
  strings_required <- concepts %>%
    filter(omop_variable %in% c("concept_name", "concept_code"))
  ancestor_concept_ids_required <- concepts %>%
    filter(omop_variable == "ancestor_concept_id")

  concept_id_expression <-
    concept_ids_required %>%
    reframe(
      concept_id = glue("{variable_names$concept_id_var} IN (",
                        glue_collapse(unique(concept_id), sep = ", "),
                        ")")) %>%
    pull(concept_id)

  string_search_expression <-
    strings_required %>%
    distinct(omop_variable, concept_id) %>%
    reframe(
      concept_id =
        glue("LOWER({omop_variable}) LIKE '%",
             glue_collapse(tolower(concept_id),
                           sep = "%' OR LOWER({omop_variable}) LIKE '%"),
             "%'")) %>%
    pull(concept_id)

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

  expressions <- c()
  if (nrow(concept_ids_required) > 0) expressions <- c(expressions, concept_id_expression)
  if (nrow(strings_required) > 0) expressions <- c(expressions, string_search_expression)
  if (nrow(ancestor_concept_ids_required) > 0) expressions <- c(expressions, ancestor_expression)

  if (length(expressions) > 0) {
    paste0("(", paste(expressions, collapse = " OR "), ")")
  } else {
    "false"
  }
}


#' Build CASE WHEN expressions for count-based variables.
#'
#' Generates COUNT(CASE WHEN ...) expressions for: value_as_concept_id,
#' concept_name, concept_code, ancestor_concept_id, and blank omop_variable.
#' Numeric variables (value_as_number) are handled via long-format queries
#' and R-side pivoting, so they are excluded.
#'
#' @param concepts Concepts dataframe (already filtered to one table,
#'   non-numeric types only).
#' @param concept_id_var The concept_id column name for this table.
#' @param table_id_var The row ID column name for this table.
#'
#' @return A SQL fragment string with COUNT expressions, or "".
#' @keywords internal
variables_query <- function(concepts,
                            concept_id_var,
                            table_id_var = ""){

  non_numeric_concepts <-
    concepts %>%
    filter((omop_variable == "value_as_concept_id" |
              is.na(omop_variable))) %>%
    group_by(short_name, omop_variable, additional_filter_variable_name) %>%
    summarise(
      concept_id = glue_collapse(unique(concept_id), sep = ", "),
      concept_id_value = glue_collapse(concept_id_value, sep = ", "),
      additional_filter_value = {
        vals <- additional_filter_value[!is.na(additional_filter_value)]
        if (length(vals) == 0) NA_character_
        else glue("'", glue_collapse(vals, sep = "', '"), "'")
      },
      .groups = "drop"
    ) %>%
    mutate(
      value_as_concept_id_query = if_else(
        omop_variable == "value_as_concept_id",
        glue("AND value_as_concept_id IN ({concept_id_value})"), "", ""),
      additional_filter_query =
        if_else(!is.na(additional_filter_variable_name),
                glue("AND {additional_filter_variable_name} IN ({additional_filter_value})"),
                "", ""),
      count_query = glue(
        ", COUNT ( CASE WHEN {concept_id_var} IN ({concept_id})
           {value_as_concept_id_query}
           {additional_filter_query}
           THEN {table_id_var}
           END ) AS count_{short_name}"))

  non_numeric_string_search_concepts <-
    concepts %>%
    filter(omop_variable %in% c("concept_name", "concept_code")) %>%
    group_by(short_name, omop_variable, additional_filter_variable_name) %>%
    summarise(
      concept_id = glue_collapse(tolower(unique(concept_id)),
                                 sep = glue("%' OR LOWER(t.{omop_variable}) LIKE '%")),
      additional_filter_value = {
        vals <- additional_filter_value[!is.na(additional_filter_value)]
        if (length(vals) == 0) NA_character_
        else glue("'", glue_collapse(vals, sep = "', '"), "'")
      },
      .groups = "drop"
    ) %>%
    mutate(
      additional_filter_query =
        if_else(!is.na(additional_filter_variable_name),
                glue("AND {additional_filter_variable_name} IN ({additional_filter_value})"),
                "", ""),
      count_query = glue(
        ", COUNT ( CASE WHEN LOWER(t.{omop_variable}) LIKE '%{concept_id}%'
           {additional_filter_query}
           THEN {table_id_var}
           END ) AS count_{short_name}"))

  ancestor_concepts <-
    concepts %>%
    filter(omop_variable %in% c("ancestor_concept_id")) %>%
    group_by(short_name, omop_variable, additional_filter_variable_name) %>%
    summarise(
      concept_id = glue_collapse(unique(concept_id), sep = ", "),
      additional_filter_value = {
        vals <- additional_filter_value[!is.na(additional_filter_value)]
        if (length(vals) == 0) NA_character_
        else glue("'", glue_collapse(vals, sep = "', '"), "'")
      },
      .groups = "drop"
    ) %>%
    mutate(
      additional_filter_query =
        if_else(!is.na(additional_filter_variable_name),
                glue("AND {additional_filter_variable_name} IN ({additional_filter_value})"),
                "", ""),
      count_query = glue(
        ", COUNT ( CASE WHEN {concept_id_var} in (SELECT descendant_concept_id FROM
            @schema.concept_ancestor WHERE
            ancestor_concept_id IN ({concept_id}))
            {additional_filter_query}
            THEN {table_id_var} END) AS count_{short_name}"))

  all_queries <- c(
    non_numeric_concepts$count_query,
    non_numeric_string_search_concepts$count_query,
    ancestor_concepts$count_query
  )

  if (length(all_queries) == 0) return("")

  glue(glue_collapse(all_queries, sep = "\n"), "\n")
}

# Translates the left lateral join between postgres and sql server for drug table.
translate_drug_join <- function(dialect){

  if (dialect == "postgresql"){
    glue(
      "LEFT JOIN LATERAL
      generate_series(
       t.drug_start
      ,t.drug_end
      ) AS gs(time_in_icu) ON TRUE")
  } else if (dialect == "sql server"){
    glue(
      "OUTER APPLY
      generate_series(
      t.drug_start
      , t.drug_end
      ) AS time_in_icu")
  } else {
    stop("Unsupported dialect for drug join: '", dialect,
         "'. This should have been caught by get_score_variables().")
  }
}


#' Build a concept_id -> short_name mapping for R-side pivoting.
#'
#' The long-format queries return rows keyed by concept_id. R needs this
#' map to create min_{short_name}, max_{short_name}, unit_{short_name}
#' columns.
#'
#' Multiple concept_ids can map to the same short_name (e.g. two LOINC
#' codes for heart rate). When additional_filter_variable_name is used,
#' the same concept_id can even map to different short_names depending
#' on the filter column value (e.g. concept 4301868 for heart rate vs
#' pulse, distinguished by measurement_source_value).
#'
#' @param concepts The concepts dataframe (already filtered by score).
#'
#' @return A data.table with columns: concept_id (character), short_name,
#'   additional_filter_variable_name (or NA), additional_filter_values
#'   (list column of character vectors, or NA).
#' @import data.table
#' @keywords internal
build_concept_map <- function(concepts) {

  numeric_concepts <- concepts %>%
    filter(omop_variable == "value_as_number") %>%
    select(concept_id, short_name,
           additional_filter_variable_name, additional_filter_value) %>%
    mutate(concept_id = as.character(concept_id))

  concept_map <- numeric_concepts %>%
    group_by(concept_id, short_name, additional_filter_variable_name) %>%
    summarise(
      additional_filter_values = list(unique(
        additional_filter_value[!is.na(additional_filter_value)]
      )),
      .groups = "drop"
    ) %>%
    mutate(
      additional_filter_values = lapply(additional_filter_values, function(v) {
        if (length(v) == 0) NA_character_ else v
      })
    )

  as.data.table(concept_map)
}
