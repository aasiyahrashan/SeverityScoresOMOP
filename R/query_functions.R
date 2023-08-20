#' Create a connection object to uds.
#' @param host postgres host
#' @param user postgres username
#' @param password postgres password
#'
#' @importFrom DBI dbConnect
#' @importFrom RPostgres Postgres
postgres_connect <- function(host, dbname, port, user, password){
  DBI::dbConnect(
    RPostgres::Postgres(),
    host = host,
    dbname = dbname,
    port = port,
    user = user ,
    password = password)
}

#' Queries the postgres database to get variables required for the severity score specified.
#' Uses visit detail as table containing ICU admission information. If not available, uses visit_occurrence.
#' @param postgres_conn A connection object to the postgres database
#' @param schema The name of the schema you want to query.
#' @param start_date The earliest ICU admission date/datetime. Needs to be in character format.
#' @param end_date As above, but for last date
#' @param min_day The number of days since ICU admission to get physiology data for. Starts with 0
#' @param max_day The number of days since ICU admision to get physiology data for.
#' @param dataset_name Describes which concept codes to select. Can only be CCHIC at the moment.
#' @param severity_score The name of the severity score to calculate. Only APACHE II at the moment.
#'
#' @import lubridate
#' @import DBI
#' @import dplyr
#' @import glue
#' @import readr
get_score_variables <- function(postgres_conn, schema, start_date, end_date,
                                min_day, max_day, dataset_name, severity_score){

  #### Getting the list of concept IDs required for the score, and creating SQL lines from them.
  concepts <- read_csv(system.file(glue("{dataset_name}_concepts.csv"),
                          package = "SeverityScoresOMOP")) %>%
    filter(score == severity_score) %>%
    mutate(max_query = glue(
      ", MAX(CASE WHEN m.measurement_concept_id = {concept_id} then m.value_as_number END) AS max_{short_name}"),
      min_query = glue(
        ", MIN(CASE WHEN m.measurement_concept_id = {concept_id} then m.value_as_number END) AS min_{short_name}"),
      unit_query = glue(
        ", MIN(CASE WHEN m.measurement_concept_id = {concept_id} then c_unit.concept_name END) AS unit_{short_name}"))

  ## Collapsing all the queries into a string.
  variables_required <- glue(glue_collapse(concepts$max_query, sep = "\n"), "\n",
                      glue_collapse(concepts$min_query, sep = "\n"), "\n",
                      glue_collapse(concepts$unit_query, sep = "\n"))

  #### Importing the rest of the query from the text file.
  raw_sql <- readr::read_file(system.file("physiology_variables.sql",
                                          package = "SeverityScoresOMOP")) %>%
    glue(schema = schema,
         start_date = start_date,
         end_date = end_date,
         min_day = min_day,
         max_day = max_day,
         variables_required = variables_required)

#### Running the query
  data <- dbGetQuery(postgres_conn, raw_sql)
  as_tibble(data)
}
