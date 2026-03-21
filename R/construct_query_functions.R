# =============================================================================
# construct_query_functions.R
#
# Functions for building SQL query components: database connections, windowing,
# age calculation, WHERE clauses, CASE WHEN aggregation, dialect translation,
# and concept mapping for R-side pivoting.
# =============================================================================

#' Create a connection to an OMOP-compliant database.
#'
#' @param driver Driver name: "PostgreSQL" or a driver string for odbc.
#' @param host Host/server name.
#' @param dbname Database name.
#' @param port Port number.
#' @param user Username.
#' @param password Password.
#'
#' @return A DBI connection object.
#' @importFrom DBI dbConnect
#' @importFrom RPostgres Postgres
#' @importFrom odbc odbc
#' @export
omop_connect <- function(driver, host, dbname, port, user, password) {
  if (driver == "PostgreSQL") {
    dbConnect(Postgres(),
              host = host, dbname = dbname, port = port,
              user = user, password = password)
  } else {
    dbConnect(odbc(),
              driver = driver, server = host, database = dbname,
              uid = user, pwd = password)
  }
}


#' Build a SQL windowing expression for time_in_icu.
#'
#' @param window_start_point Either "calendar_date" or "icu_admission_time".
#' @param time_variable SQL expression for the datetime column.
#' @param date_variable SQL expression for the date column.
#' @param cadence Window size in hours. Must be > 0.
#'
#' @return A SQL expression string computing time_in_icu.
#' @importFrom glue glue
#' @keywords internal
window_query <- function(window_start_point, time_variable,
                         date_variable, cadence) {

  if (!(window_start_point %in% c("calendar_date", "icu_admission_time"))) {
    stop("window_start_point must be either 'calendar_date' or 'icu_admission_time'")
  }
  if (window_start_point == "calendar_date" && cadence != 24) {
    stop("When window_start_point = 'calendar_date', cadence must be 24.")
  }
  if (!is.numeric(cadence) || cadence <= 0) {
    stop("cadence must be a number > 0")
  }

  if (window_start_point == "calendar_date") {
    glue(" DATEDIFF(dd, adm.icu_admission_datetime, {date_variable})")
  } else {
    glue(" FLOOR(DATEDIFF(MINUTE, adm.icu_admission_datetime, ",
         "{time_variable}) / ({cadence} * 60))")
  }
}


#' Build a SQL expression for calculating patient age.
#'
#' @param age_method Either "dob" (full date of birth) or "year_only".
#'
#' @return A SQL expression string.
#' @keywords internal
age_query <- function(age_method) {

  if (!(age_method %in% c("dob", "year_only"))) {
    stop("age_method must be either 'dob' or 'year_only'")
  }

  if (age_method == "dob") {
    paste0(" DATEDIFF(yyyy,",
           " COALESCE(birth_datetime, DATEFROMPARTS(year_of_birth,",
           " COALESCE(month_of_birth, '06'),",
           " COALESCE(day_of_birth, '01'))),",
           " icu_admission_datetime) as age")
  } else {
    " YEAR(icu_admission_datetime) - year_of_birth AS age"
  }
}


#' Build a WHERE clause filtering by concept IDs, string search, or ancestors.
#'
#' Combines three types of filters with OR:
#'   - Direct concept_id IN (...) for standard and value_as_concept_id rows
#'   - LOWER(concept_name/code) LIKE '%...%' for string search rows
#'   - concept_id IN (SELECT ... FROM concept_ancestor) for ancestor rows
#'
#' Returns "false" if no concepts match, so the query returns zero rows.
#'
#' @param concepts Concepts dataframe (will be filtered to table_name internally).
#' @param variable_names Variable names dataframe.
#' @param table_name OMOP table name to filter by.
#'
#' @return A SQL expression string.
#' @importFrom glue glue glue_collapse
#' @importFrom dplyr filter reframe distinct pull
#' @keywords internal
where_clause <- function(concepts, variable_names, table_name) {

  concepts <- concepts[concepts$table == table_name, ]
  vn <- variable_names[variable_names$table == table_name, ]

  concept_ids_required <- concepts[
    !(concepts$omop_variable %in% c("concept_name", "concept_code",
                                    "ancestor_concept_id")) |
      is.na(concepts$omop_variable), ]

  strings_required <- concepts[
    concepts$omop_variable %in% c("concept_name", "concept_code"), ]

  ancestors_required <- concepts[
    concepts$omop_variable == "ancestor_concept_id" &
      !is.na(concepts$omop_variable), ]

  expressions <- character(0)

  if (nrow(concept_ids_required) > 0) {
    ids <- unique(concept_ids_required$concept_id)
    expressions <- c(expressions,
                     glue("{vn$concept_id_var} IN ({glue_collapse(ids, sep = ', ')})"))
  }

  if (nrow(strings_required) > 0) {
    str_expr <- strings_required %>%
      distinct(omop_variable, concept_id) %>%
      reframe(
        expr = glue("LOWER({omop_variable}) LIKE '%",
                    glue_collapse(tolower(concept_id),
                                  sep = "%' OR LOWER({omop_variable}) LIKE '%"),
                    "%'")) %>%
      pull(expr)
    expressions <- c(expressions, str_expr)
  }

  if (nrow(ancestors_required) > 0) {
    anc_ids <- unique(ancestors_required$concept_id)
    expressions <- c(expressions,
                     glue("{vn$concept_id_var} IN (SELECT descendant_concept_id FROM ",
                          "@schema.concept_ancestor WHERE ancestor_concept_id IN (",
                          "{glue_collapse(anc_ids, sep = ', ')}))"))
  }

  if (length(expressions) > 0) {
    paste0("(", paste(expressions, collapse = " OR "), ")")
  } else {
    "false"
  }
}


#' Build COUNT(CASE WHEN ...) expressions for non-numeric variables.
#'
#' Handles value_as_concept_id, concept_name, concept_code,
#' ancestor_concept_id, and blank omop_variable. Returns one
#' count_{short_name} column per variable.
#'
#' @param concepts Concepts dataframe (pre-filtered to one table,
#'   non-numeric types only).
#' @param concept_id_var Column name for concept IDs in this table.
#' @param table_id_var Column name for row IDs in this table.
#'
#' @return A SQL fragment string, or "".
#' @importFrom glue glue glue_collapse
#' @importFrom dplyr filter group_by summarise mutate if_else
#' @keywords internal
variables_query <- function(concepts, concept_id_var, table_id_var = "") {

  # --- value_as_concept_id and blank omop_variable ---
  non_numeric <- concepts %>%
    filter(omop_variable == "value_as_concept_id" | is.na(omop_variable)) %>%
    group_by(short_name, omop_variable, additional_filter_variable_name) %>%
    summarise(
      concept_id = glue_collapse(unique(concept_id), sep = ", "),
      concept_id_value = glue_collapse(concept_id_value, sep = ", "),
      additional_filter_value = {
        vals <- additional_filter_value[!is.na(additional_filter_value)]
        if (length(vals) == 0) NA_character_
        else glue("'", glue_collapse(vals, sep = "', '"), "'")
      },
      .groups = "drop") %>%
    mutate(
      val_filter = if_else(
        omop_variable == "value_as_concept_id",
        glue(" AND value_as_concept_id IN ({concept_id_value})"), "", ""),
      add_filter = if_else(
        !is.na(additional_filter_variable_name),
        glue(" AND {additional_filter_variable_name} IN ({additional_filter_value})"),
        "", ""),
      sql = glue(", COUNT(CASE WHEN {concept_id_var} IN ({concept_id})",
                 "{val_filter}{add_filter}",
                 " THEN {table_id_var} END) AS count_{short_name}"))

  # --- String search (concept_name / concept_code) ---
  # concept_name/concept_code are pre-joined into the filtered temp table
  # when string search variables exist for that table.
  string_search <- concepts %>%
    filter(omop_variable %in% c("concept_name", "concept_code")) %>%
    group_by(short_name, omop_variable, additional_filter_variable_name) %>%
    summarise(
      search_pattern = glue_collapse(
        tolower(unique(concept_id)),
        sep = glue("%' OR LOWER(t.{omop_variable}) LIKE '%")),
      additional_filter_value = {
        vals <- additional_filter_value[!is.na(additional_filter_value)]
        if (length(vals) == 0) NA_character_
        else glue("'", glue_collapse(vals, sep = "', '"), "'")
      },
      .groups = "drop") %>%
    mutate(
      add_filter = if_else(
        !is.na(additional_filter_variable_name),
        glue(" AND {additional_filter_variable_name} IN ({additional_filter_value})"),
        "", ""),
      sql = glue(", COUNT(CASE WHEN LOWER(t.{omop_variable}) ",
                 "LIKE '%{search_pattern}%'{add_filter}",
                 " THEN {table_id_var} END) AS count_{short_name}"))

  # --- Ancestor concept_id ---
  ancestors <- concepts %>%
    filter(omop_variable == "ancestor_concept_id") %>%
    group_by(short_name, omop_variable, additional_filter_variable_name) %>%
    summarise(
      concept_id = glue_collapse(unique(concept_id), sep = ", "),
      additional_filter_value = {
        vals <- additional_filter_value[!is.na(additional_filter_value)]
        if (length(vals) == 0) NA_character_
        else glue("'", glue_collapse(vals, sep = "', '"), "'")
      },
      .groups = "drop") %>%
    mutate(
      add_filter = if_else(
        !is.na(additional_filter_variable_name),
        glue(" AND {additional_filter_variable_name} IN ({additional_filter_value})"),
        "", ""),
      sql = glue(", COUNT(CASE WHEN {concept_id_var} IN ",
                 "(SELECT descendant_concept_id FROM @schema.concept_ancestor ",
                 "WHERE ancestor_concept_id IN ({concept_id})){add_filter}",
                 " THEN {table_id_var} END) AS count_{short_name}"))

  all_sql <- c(non_numeric$sql, string_search$sql, ancestors$sql)
  if (length(all_sql) == 0) return("")

  glue(glue_collapse(all_sql, sep = "\n"), "\n")
}


#' Translate a lateral join for the Drug table between PostgreSQL and SQL Server.
#'
#' @param dialect "postgresql" or "sql server".
#' @return A SQL fragment string.
#' @importFrom glue glue
#' @keywords internal
translate_drug_join <- function(dialect) {
  if (dialect == "postgresql") {
    glue("LEFT JOIN LATERAL\n",
         "      generate_series(t.drug_start, t.drug_end)\n",
         "      AS gs(time_in_icu) ON TRUE")
  } else if (dialect == "sql server") {
    glue("OUTER APPLY\n",
         "      generate_series(t.drug_start, t.drug_end)\n",
         "      AS time_in_icu")
  } else {
    stop("Unsupported dialect: '", dialect, "'")
  }
}


#' Build a concept_id -> short_name mapping for R-side pivoting.
#'
#' Long-format queries return rows keyed by concept_id. R needs this
#' map to pivot them into min_{short_name}, max_{short_name}, unit_{short_name}.
#'
#' When additional_filter_variable_name is used, the same concept_id can map
#' to different short_names depending on the filter column value.
#'
#' @param concepts The concepts dataframe (already filtered by score).
#'
#' @return A data.table with columns: concept_id (character), short_name,
#'   filter_col (character or NA), filter_vals (list of character vectors).
#'
#' @import data.table
#' @importFrom dplyr filter select mutate group_by summarise
#' @keywords internal
build_concept_map <- function(concepts) {

  num <- concepts[concepts$omop_variable == "value_as_number" &
                    !is.na(concepts$omop_variable), ]

  if (nrow(num) == 0) return(data.table(
    concept_id = character(0), short_name = character(0),
    filter_col = character(0), filter_vals = list()))

  num$concept_id <- as.character(num$concept_id)

  result <- num %>%
    group_by(concept_id, short_name, additional_filter_variable_name) %>%
    summarise(
      filter_vals = list(unique(
        additional_filter_value[!is.na(additional_filter_value)])),
      .groups = "drop") %>%
    mutate(
      filter_col = additional_filter_variable_name,
      filter_vals = lapply(filter_vals, function(v) {
        if (length(v) == 0) NA_character_ else v
      })) %>%
    select(concept_id, short_name, filter_col, filter_vals)

  as.data.table(result)
}
