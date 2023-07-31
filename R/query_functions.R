#' Create a connection object to uds.
#' @param host postgres host
#' @param user postgres username
#' @param password postgres password
postgres_connect <- function(host, user, password){
  DBI::dbConnect(
    RPostgres::Postgres(),
    host = host,
    dbname = dbname,
    user = user ,
    password = password,
    port = 5432)
}

#' Queries the postgres database to get variables required for the severity score specified.
#' Fixes the postgres timezone issue.
#' @param postgres_conn A connection object to the postgres database
#' @param schema The name of the schema you want to query.
#' @param start_date The date/datetime of the first admission. Needs to be in character format.
#' @param end_date As above, but for last date
#' @param dataset_name Describes which concept codes to select. Can only be CCHIC at the moment.
#' @param severity_score The name of the severity score to calculate. Only APACHE2 at the moment.
#'
#' @import lubridate
#' @import DBI
#' @import tidyverse
#' @import glue
get_score_variables <- function(postgres_conn, schema, start_date, end_date,
                                 dataset_name, severity_score){


  #### Getting the list of concept IDs required for the score.
  concept_ids <- read_csv(system.file(glue("{dataset_name}_concepts.csv")),
                          package = "SeverityScoreOMOP") %>%
    filter(score == severity_score)

  # Getting the query and substituting parameters.
  raw_sql <- readr::read_file(system.file("physiology_variables.sql",
                                          package = "SeverityScoresOMOP")) %>%
    glue(schema = schema,
         start_date = start_date,
         end_date = end_date,
         concept_ids = concept_ids)

####

  # run the query
  uds <- dbGetQuery(uds_conn, raw_sql) %>%
    mutate_if(is.instant, ~fix_timezones_for_imported_dates(.x, source = "star")) %>%
    mutate_if(is.character, ~str_trim(.)) %>%
    mutate(date_of_birth = as.Date(date_of_birth),
           alive = case_when(alive == "FALSE" ~ "Deceased",
                             alive == "TRUE" ~ "Alive"),
           sex = case_when(sex == "I" ~ "N",
                           TRUE ~ sex))

  as_tibble(uds)
}
