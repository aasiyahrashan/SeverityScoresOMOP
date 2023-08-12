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
  concepts <- read_csv(system.file(glue("{dataset_name}_concepts.csv"),
                          package = "SeverityScoresOMOP")) %>%
    filter(score == severity_score)

  concept_ids <- concepts %>%
    summarise(concept_ids = toString(concept_id)) %>%
    pull(concept_ids)

  ###### Creating a query to convert the table from long to wide
  # Construct the dynamic SQL query to pivot the table
  pivot_query <- glue::glue("
  SELECT {id_column},
         {paste(paste('MAX(CASE WHEN', {{variable_column}}, '=', '''', var, '''', 'THEN', {{value_column}}, 'END) AS', var), collapse = ', ')}
  FROM {long_table}
  GROUP BY {id_column}
")

  pivot_query <- sprintf("
  SELECT %s,
         %s
  FROM %s
  GROUP BY %s
", id_column,
    paste(paste('MAX(CASE WHEN', variable_column, '=', "'", var, "'", 'THEN', value_column, 'END) AS', var), collapse = ', '),
    long_table,
    id_column
  )

  pivot_query <- glue_sql("
  SELECT {id_column},
         {glue_collapse(glue('MAX(CASE WHEN {variable_column} = ', .x, ' THEN {value_column} END) AS {var}'), ', ')}
  FROM {long_table}
  GROUP BY {id_column}
", .con = con, .open = "", .sep = ", ", .close = "")

  # Getting the query and substituting parameters.
  raw_sql <- readr::read_file(system.file("physiology_variables.sql",
                                          package = "SeverityScoresOMOP")) %>%
    glue(schema = schema,
         start_date = start_date,
         end_date = end_date,
         concept_ids = concept_ids)


  ###### Trying the variable names query to check it's not supposed to replace properly.
  variable_names_query <- glue_sql("
  SELECT DISTINCT {variable_column}
  FROM {schema_name}.{long_table}
", .con = postgres_conn)

  variable_names <- dbGetQuery(postgres_conn, as.character(variable_names_query))[[1]]



#### Getting physiology variables.

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
