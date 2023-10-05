#' Create a connection object to uds.
#' @param driver odbc driver eg. "SQL Server", "PostgreSQL Driver", etc.
#' @param host host/server name
#' @param dbname name of database in server
#' @param port database port
#' @param user username
#' @param password password
#'
#' @importFrom DBI dbConnect
#' @importFrom RPostgres Postgres
#' @importFrom odbc odbc
#' @export
omop_connect <- function(driver, host, dbname, port, user, password){
  if (driver = "PostgreSQL"){
    DBI::dbConnect(
      RPostgres::Postgres(),
      host = host,
      dbname = dbname,
      port = port,
      user = user ,
      password = password)
  } else {
    DBI::dbConnect(
      odbc::odbc(),
      driver = driver,
      server = host,
      database = dbname,
      uid = user,
      pwd = password)
  }
}

#' Queries a database to get variables required for a specified severity score. Assumes visit detail
#' table contains ICU admission information. If not available, uses visit_occurrence.
#' @param conn A connection object to a database
#' @param schema The name of the schema you want to query.
#' @param start_date The earliest ICU admission date/datetime. Needs to be in character format.
#' @param end_date As above, but for last date
#' @param min_day The number of days since ICU admission to get physiology data for. Starts with 0
#' @param max_day The number of days since ICU admission to get physiology data for.
#' @param dataset_name Describes which concept codes to select. Has to match *_concepts.csv file name.
#' @param severity_score The name of the severity score to calculate. Only APACHE II at the moment.
#'
#' @import lubridate
#' @import DBI
#' @import dplyr
#' @import glue
#' @import readr
#' @export
get_score_variables <- function(conn, schema, start_date, end_date,
                                min_day, max_day, dataset_name, severity_score){

  #### Getting the list of concept IDs required for the score, and creating SQL lines from them.
  concepts <- read_csv(system.file(glue("{dataset_name}_concepts.csv"),
                                   package = "SeverityScoresOMOP")) %>%
    filter(score == severity_score)

  ##### Getting concepts stored in the measurement table.
  measurement_concepts <-
    concepts %>%
    filter(table == "Measurement") %>%
    #### GCS can sometimes be stored as a concept ID instead of number.
    #### These need a separate query.
    filter(!(short_name %in% c("gcs_eye", "gcs_motor", "gcs_verbal") &
               omop_variable == "value_as_concept_id")) %>%
    mutate(max_query = glue(
      ", MAX(CASE WHEN m.measurement_concept_id = {concept_id} then m.{omop_variable} END) AS max_{short_name}"),
      min_query = glue(
        ", MIN(CASE WHEN m.measurement_concept_id = {concept_id} then m.{omop_variable} END) AS min_{short_name}"),
      unit_query = glue(
        ", MIN(CASE WHEN m.measurement_concept_id = {concept_id} then c_unit.concept_name END) AS unit_{short_name}"))

  ## Collapsing all the queries into a string.
  variables_required <- glue(glue_collapse(measurement_concepts$max_query, sep = "\n"), "\n",
                             glue_collapse(measurement_concepts$min_query, sep = "\n"), "\n",
                             glue_collapse(measurement_concepts$unit_query, sep = "\n"))

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
  data <- dbGetQuery(conn, raw_sql)

  ######### Doing GCS separately if stored as concept ID instead of number.
  ######### Otherwise, there's no way of getting the min and max
  gcs_concepts <- concepts %>%
    filter((short_name %in% c("gcs_eye", "gcs_motor", "gcs_verbal") &
              omop_variable == "value_as_concept_id"))

  if(nrow(gcs_concepts > 0)){
    ###### The SQL file currently has the GCS LOINC concepts hardocded.
    ###### I plan to construct it from here when I have time.
    raw_sql <- readr::read_file(system.file("gcs_if_stored_as_concept.sql",
                                            package = "SeverityScoresOMOP")) %>%
      glue(schema = schema,
           start_date = start_date,
           end_date = end_date,
           min_day = min_day,
           max_day = max_day)
    #### Running the query
    gcs_data <- dbGetQuery(conn, raw_sql)

    ### Don't like joining here, but not much choice.
    data <- left_join(data, gcs_data,
                      by = c("person_id", "visit_occurrence_id",
                             "visit_detail_id", "days_in_icu"))
  }

  ########## Some concepts may be stored in the observation table.
  ########## Using a completely separate query for these and joining the results after
  ########## TODO - try getting these into one query so the joining can happen in SQL.
  ########## Other datasets may store numeric values in the observation table. This is not currently supported.
  observation_concepts <- concepts %>%
    filter(table == "Observation" & omop_variable == "value_as_concept_id")

  if (nrow(observation_concepts) > 0){
    #### Most of these use several concepts IDs to represent the same score variable.
    #### Eg, CCAA uses 3 codes for renal failure. Collapsing those here.
    #### The query returns the number of rows matching the concept IDs provided.
    observation_concepts <- observation_concepts %>%
      group_by(short_name, concept_id) %>%
      summarise(concept_id_value =
                  glue("'", glue_collapse(concept_id_value, sep = "', '"), "'")) %>%
      mutate(count_query = glue(
        ", COUNT (CASE WHEN o.observation_concept_id = {concept_id}
        AND o.value_as_concept_id IN ({concept_id_value}) THEN o.observation_id END)
        as count_{short_name}"))

    ## Collapsing all the queries into a string.
    observation_variables_required <-
      glue(glue_collapse(observation_concepts$count_query, sep = "\n"), "\n")

    #### Importing the rest of the query from the text file.
    raw_sql <- readr::read_file(system.file("history_variables.sql",
                                            package = "SeverityScoresOMOP")) %>%
      glue(schema = schema,
           start_date = start_date,
           end_date = end_date,
           observation_variables_required = observation_variables_required)

    #### Running the query
    observation_data <- dbGetQuery(conn, raw_sql)

    #### Don't like having to merge here, but doing it for now.
    data <- left_join(data, observation_data,
                      by = c("person_id", "visit_occurrence_id", "visit_detail_id"))
  }

  as_tibble(data)
}

