#' Create a connection object to on OMOP compliant database.
#' @param driver driver eg. "SQL Server", "PostgreSQL", etc.
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
omop_connect <-
  function(driver, host, dbname, port, user, password) {
    if (driver == "PostgreSQL") {
      DBI::dbConnect(
        RPostgres::Postgres(),
        host = host,
        dbname = dbname,
        port = port,
        user = user ,
        password = password
      )
    } else {
      DBI::dbConnect(
        odbc::odbc(),
        driver = driver,
        server = host,
        database = dbname,
        uid = user,
        pwd = password
      )
    }
  }

#' Queries a database to get variables required for a specified severity score.
#' Assumes visit detail table contains ICU admission information. If not
#' available, uses visit_occurrence.
#'
#' @param conn A connection object to a database
#' @param dialect dialect A dialect supported by SQLRender
#' @param schema The name of the schema you want to query.
#' @param start_date
#' The earliest ICU admission date/datetime. Needs to be in character format.
#' @param end_date
#' As above, but for last date
#' @param min_day
#' Days since ICU admission to get physiology data for, starting with 0.
#' @param max_day
#' Days since ICU admission to get physiology data for.
#' @param mapping_path
#' Path to the custom *_concepts.tsv file containing score to OMOP mappings.
#' Should match the example_concepts.tsv file format.
#' @param severity_score
#' The name of the severity score to calculate. Only APACHE II at the moment.
#' @param age_method
#' Either 'year_only' or 'dob'. Decides if age is calculated from year of birth or full DOB
#' Default is 'dob'
#' @param window_method
#'  This decides how to determine days spent in ICU. Options are calendar_date or 24_hrs
#' Option 1 (the default) uses the calendar date only, with day 0 being the date of admission to ICU.
#' Option 2 (should be used for CC-HIC or EHR data) divides observations into 24 hour windows from ICU admission time.

#' @import lubridate
#' @import DBI
#' @import dplyr
#' @import glue
#' @import readr
#' @export
get_score_variables <- function(conn, dialect, schema,
                                start_date, end_date,
                                min_day, max_day,
                                concepts_file_path,
                                severity_score,
                                age_method = "dob",
                                window_method = "calendar_date") {
  # Editing the date variables to keep explicit single quote for SQL
  start_date <- single_quote(start_date)
  end_date <- single_quote(end_date)

  # Creating windowing query
  if (!(window_method %in% c("calendar_date", "24_hrs"))) {
    stop("Error: window_method must be either 'calendar_date' or '24_hrs'")
    }
  window_measurement <- ifelse(
    window_method == "calendar_date",
    " DATEDIFF(dd, adm.icu_admission_datetime, COALESCE(m.measurement_datetime, m.measurement_date))",
    " DATEDIFF(MINUTE, adm.icu_admission_datetime, COALESCE(m.measurement_datetime, m.measurement_date)) / (24 * 60)") %>%
    SqlRender::translate(tolower(dialect))
  window_observation <- ifelse(
    window_method == "calendar_date",
    " DATEDIFF(dd, adm.icu_admission_datetime, COALESCE(o.observation_datetime, o.observation_date))",
    " DATEDIFF(MINUTE, adm.icu_admission_datetime, COALESCE(o.observation_datetime, o.observation_date)) / (24 * 60)") %>%
    SqlRender::translate(tolower(dialect))
  window_condition <- ifelse(
    window_method == "calendar_date",
    " DATEDIFF(dd, adm.icu_admission_datetime, COALESCE(co.condition_start_datetime, co.condition_start_date))",
    " DATEDIFF(MINUTE, adm.icu_admission_datetime, COALESCE(co.condition_start_datetime, co.condition_start_date)) / (24 * 60)") %>%
    SqlRender::translate(tolower(dialect))
  window_procedure <- ifelse(
    window_method == "calendar_date",
    " DATEDIFF(dd, adm.icu_admission_datetime, COALESCE(po.procedure_datetime, po.procedure_date))",
    " DATEDIFF(MINUTE, adm.icu_admission_datetime, COALESCE(po.procedure_datetime, po.procedure_date)) / (24 * 60)") %>%
    SqlRender::translate(tolower(dialect))

  # Getting the list of concept IDs required and creating SQL lines from them.
  concepts <- read_delim(file = concepts_file_path) %>%
    filter(score == severity_score)

  # First getting admission information
  # Age query
  if (!(age_method %in% c("dob", "year_only"))) {
  stop("Error: age_method must be either 'dob' or 'year_only'")
}
  age_query <- ifelse(
    age_method == "dob",
  " DATEDIFF(yyyy,
			  COALESCE(p.birth_datetime, DATEFROMPARTS(p.year_of_birth, COALESCE(p.month_of_birth, '06'),
          COALESCE(p.day_of_birth, '01'))),
			  COALESCE(vd.visit_detail_start_datetime, vd.visit_detail_start_date,
        vo.visit_start_datetime, vo.visit_start_date)) as age",
  " YEAR(COALESCE(vd.visit_detail_start_datetime, vd.visit_detail_start_date,
                   vo.visit_start_datetime, vo.visit_start_date)) - p.year_of_birth AS age") %>%
  SqlRender::translate(tolower(dialect))

  # Admission information
  raw_sql <- readr::read_file(
    system.file("admission_details.sql", package = "SeverityScoresOMOP")) %>%
    SqlRender::translate(tolower(dialect)) %>%
    SqlRender::render(schema             = schema,
                      age_query          = age_query,
                      start_date         = start_date,
                      end_date           = end_date)

  adm_details <- dbGetQuery(conn, raw_sql)

  # Getting concepts stored in the measurement table.
  measurement_concepts <- concepts %>%
    filter(table == "Measurement") %>%
    #### GCS can sometimes be stored as a concept ID instead
    #### of a number. These need a separate query.
    filter(!(short_name %in% c("gcs_eye", "gcs_motor", "gcs_verbal") &
               omop_variable == "value_as_concept_id")) %>%
    mutate(
      max_query = glue(
        ", MAX(CASE WHEN m.measurement_concept_id = {concept_id}
                    THEN m.{omop_variable}
               END) AS max_{short_name}"
      ),
      min_query = glue(
        ", MIN(CASE WHEN m.measurement_concept_id = {concept_id}
                    THEN m.{omop_variable}
               END) AS min_{short_name}"
      ),
      unit_query = glue(
        ", MIN(CASE WHEN m.measurement_concept_id = {concept_id}
                    THEN c_unit.concept_name
               END) AS unit_{short_name}"
      ))

  ## Collapsing all the queries into a string.
  variables_required <-
    glue(glue_collapse(measurement_concepts$max_query, sep = "\n"),
         "\n",
         glue_collapse(measurement_concepts$min_query, sep = "\n"),
         "\n",
         glue_collapse(measurement_concepts$unit_query, sep = "\n"))

  # Importing the rest of the query from the text file.
  raw_sql <- readr::read_file(
    system.file("physiology_variables.sql", package = "SeverityScoresOMOP")) %>%
    SqlRender::translate(tolower(dialect)) %>%
    SqlRender::render(schema             = schema,
                      start_date         = start_date,
                      end_date           = end_date,
                      min_day            = min_day,
                      max_day            = max_day,
                      window_measurement = window_measurement,
                      variables_required = variables_required)

  #### Running the query
  data <- dbGetQuery(conn, raw_sql)

  # Doing GCS separately if stored as concept ID instead of number.
  # Otherwise, there's no way of getting the min and max
  gcs_concepts <- concepts %>%
    filter((short_name %in% c("gcs_eye", "gcs_motor", "gcs_verbal") &
              omop_variable == "value_as_concept_id"))

  if (nrow(gcs_concepts > 0)) {
    ###### The SQL file currently has the GCS LOINC concepts hardcoded.
    ###### I plan to construct it from here when I have time.
    raw_sql <-
      readr::read_file(system.file("gcs_if_stored_as_concept.sql",
                                   package = "SeverityScoresOMOP")) %>%
      SqlRender::translate(tolower(dialect)) %>%
      SqlRender::render(schema = schema,
                        start_date = start_date,
                        end_date = end_date,
                        min_day = min_day,
                        max_day = max_day,
                        window_measurement = window_measurement)

    #### Running the query
    gcs_data <- dbGetQuery(conn, raw_sql)

    ### Don't like joining here, but not much choice.
    data <- full_join(data,
                      gcs_data,
                      by = c("person_id",
                             "visit_occurrence_id",
                             "visit_detail_id",
                             "days_in_icu"))
  }

  # Getting comorbidities ---------------------------------------------------
  # Some of these concepts may be stored in the observation table as a single
  # variable, some in the condition_occurrence or
  # procedure_occurrence table as multiple variables.
  # This difference depends on the ETL paradigm applied.
  # Queries are created observation table, condition_occurrence table,
  # procedure_occurrence and, if
  # applicable, the visit_detail table for emergency admission count.

  # TODO - get these into one query so joining can happen in SQL.
  # Numeric values in the observation table are currently unsupported

  # Get possible comorbidity concept_ids from {dataset_name}_concepts.csv file.
  observation_concepts <- concepts %>%
    filter(table == "Observation" &
             (omop_variable == "value_as_concept_id" |
             is.na(omop_variable)))

  condition_concepts <- concepts %>%
    filter(table == "Condition")

  procedure_concepts <- concepts %>%
    filter(table == "Procedure")

  # Some use a flag variable in visit_detail to record emergency admissions.
  visit_detail_concepts <- concepts %>%
    filter(table == "Visit Detail" & short_name == "emergency_admission")

  # Get all available short_names from {dataset_name}_concepts.csv file.
  required_variables <- concepts %>%
    filter(table %in% c("Observation", "Condition",
                        "Procedure", "Visit Detail")) %>%
    mutate(short_name = glue("count_{short_name}")) %>%
    distinct(short_name) %>%
    pull(.) %>%
    toString(.) %>%
    glue(",", .)

  # Initialize count query strings
  observation_variables_required  = ""
  condition_variables_required    = ""
  procedure_variables_required    = ""
  visit_detail_variables_required = ""

  #### Comorbidities from observation table
  # Most use several concepts IDs to represent the same score variable.
  # Eg, CCAA uses 3 codes for renal failure. Collapsing those here.
  # The query returns the number of rows matching the concept IDs provided.

  if (nrow(observation_concepts) > 0) {
    observation_concepts <- observation_concepts %>%
      group_by(short_name, omop_variable) %>%
      summarise(
        # This is slightly odd, but just makes sure we don't duplicate concept
        # IDs in cases where we're selecting specific values
        concept_id = ifelse(length(unique(concept_id)) == 1,
                            as.character(unique(concept_id)),
                            glue("'",
                                 glue_collapse(unique(concept_id), sep = "', '"),
                                 "'")),
        concept_id_value = glue(
          "'",
          glue_collapse(concept_id_value, sep = "', '"),
          "'"
        )
      ) %>%
      # Separate queries depending on whether the concept ID variable is filled.
      mutate(count_query = case_when(
        omop_variable == "value_as_concept_id" ~
        glue(
        ", COUNT ( CASE WHEN o.observation_concept_id = {concept_id}
                        AND o.value_as_concept_id
                        IN ({concept_id_value})
                        THEN o.observation_id
                    END ) AS count_{short_name}"
      ),
      is.na(omop_variable) ~
      glue(
        ", COUNT ( CASE WHEN o.observation_concept_id IN ({concept_id})
                        THEN o.observation_id
                    END ) AS count_{short_name}"
      )
      ))

    ## Collapsing the queries into strings.
    observation_variables_required <-
      glue(
        glue_collapse(observation_concepts$count_query, sep = "\n"),
        "\n"
      )
  }

  #### Comorbidities from condition_occurrence
  # Most use several concepts IDs to represent the same score variable.
  # E.g., CCAA uses 3 codes for renal failure. Collapsing those here.
  # The query returns the number of rows matching the concept IDs provided.
  if (nrow(condition_concepts) > 0) {
    condition_concepts <- condition_concepts %>%
      group_by(short_name) %>%
      summarise(concept_id =
                  glue("'",
                       glue_collapse(concept_id, sep = "', '"),
                       "'")
      ) %>%
      mutate(count_query =
               glue(",COUNT(CASE
                                 WHEN co.condition_concept_id IN ({concept_id})
                                 THEN co.condition_occurrence_id
                            END) AS count_{short_name}")
      )

    ## Collapsing the queries into strings.
    condition_variables_required <-
      glue(
        glue_collapse(condition_concepts$count_query, sep = "\n"),
        "\n"
      )
  }

  # Comorbidities from procedure_occurrence
  # Most use several concepts IDs to represent the same score variable.
  # E.g., CCAA uses 3 codes for renal failure. Collapsing those here.
  # The query returns the number of rows matching the concept IDs provided.
  if (nrow(procedure_concepts) > 0) {
    procedure_concepts <- procedure_concepts %>%
      group_by(short_name) %>%
      summarise(concept_id =
                  glue("'",
                       glue_collapse(concept_id, sep = "', '"),
                       "'")
      ) %>%
      mutate(count_query =
               glue(",COUNT(CASE
                                 WHEN po.procedure_concept_id IN ({concept_id})
                                 THEN po.procedure_concept_id
                            END) AS count_{short_name}")
      )

    ## Collapsing the queries into strings.
    procedure_variables_required <-
      glue(
        glue_collapse(procedure_concepts$count_query, sep = "\n"),
        "\n"
      )
  }

  # Emergency admissions from the visit detail table
  if (nrow(visit_detail_concepts) > 0) {
    emergency_admission_concept <- visit_detail_concepts$concept_id
    visit_detail_variables_required <- glue(
      ",COUNT( CASE
         WHEN vd.visit_detail_source_concept_id = {emergency_admission_concept}
         THEN vd.visit_detail_id
     END ) AS count_emergency_admission "
    )
  }

  #### Importing the rest of the query from the script file.
  raw_sql <-
    readr::read_file(system.file("history_variables.sql",
                                 package = "SeverityScoresOMOP")) %>%
    SqlRender::translate(tolower(dialect)) %>%
    SqlRender::render(
      schema = schema,
      start_date = start_date,
      end_date = end_date,
      min_day = min_day,
      max_day = max_day,
      required_variables = required_variables,
      window_observation = window_observation,
      window_condition = window_condition,
      window_procedure = window_procedure,
      observation_variables_required = observation_variables_required,
      condition_variables_required = condition_variables_required,
      procedure_variables_required = procedure_variables_required,
      visit_detail_variables_required = visit_detail_variables_required
    )

  #### Running the query
  comorbidity_data <- dbGetQuery(conn, raw_sql)
  # data <- comorbidity_data
  #### Don't like having to merge here, but doing it for now.
  data <- full_join(data,
                    comorbidity_data,
                    by = c("person_id",
                           "visit_occurrence_id",
                           "visit_detail_id",
                           "icu_admission_datetime",
                           "days_in_icu"))

  # Joining to ICU admission details to get all patients
  data <- left_join(adm_details, data,
                    by = c("person_id",
                                 "visit_occurrence_id",
                                 "visit_detail_id",
                                 "icu_admission_datetime"))

  as_tibble(data)
}
