# =============================================================================
# ground_truth_functions.R
# =============================================================================


#' Quote a SQL identifier for the target dialect.
#'
#' PostgreSQL lowercases unquoted identifiers, so mixed-case column names
#' like "PatientDurableKey" must be double-quoted. SQL Server uses square
#' brackets. This function wraps a column name for safe use in dynamic SQL.
#'
#' @param identifier Column or table name to quote.
#' @param dialect SQL dialect ("postgresql" or "sql server").
#'
#' @return The quoted identifier string.
#' @keywords internal
quote_identifier <- function(identifier, dialect) {
  if (tolower(dialect) == "sql server") {
    paste0("[", identifier, "]")
  } else {
    paste0('"', identifier, '"')
  }
}


#' Create a ground truth configuration object.
#'
#' Bundles all ground-truth-related parameters into a single object
#' for cleaner function signatures. Pass the result to
#' \code{get_score_variables(ground_truth = ...)}.
#'
#' @param df A dataframe containing ground truth ICU stay data.
#' @param joining_schema Schema name in the OMOP database containing ID mapping tables.
#' @param joining_person_table Table name in \code{joining_schema} that maps external
#'   patient identifiers to OMOP \code{person_id}.
#' @param joining_visit_table Table name in \code{joining_schema} that maps external
#'   encounter identifiers to OMOP \code{visit_occurrence_id}.
#' @param gt_person_key Column name in \code{df} for the patient identifier
#'   (e.g. \code{"PatientDurableKey"}).
#' @param omop_person_key Column name in \code{joining_person_table} that matches
#'   \code{gt_person_key} (e.g. \code{"PatientDurableKey"}).
#' @param gt_encounter_key Column name in \code{df} for the encounter identifier
#'   (e.g. \code{"EncounterKey"}).
#' @param omop_encounter_key Column name in \code{joining_visit_table} that matches
#'   \code{gt_encounter_key} (e.g. \code{"EncounterKey"}).
#' @param gt_icu_admission_col Column name in \code{df} for ICU admission datetime.
#' @param gt_icu_discharge_col Column name in \code{df} for ICU discharge datetime.
#' @param gt_hospital_admission_col Optional. Column name in \code{df} for hospital
#'   admission datetime. If NULL (default), hospital times are taken from OMOP
#'   visit_occurrence instead.
#' @param gt_hospital_discharge_col Optional. Column name in \code{df} for hospital
#'   discharge datetime. If NULL (default), hospital times are taken from OMOP
#'   visit_occurrence instead.
#'
#' @return A list of class \code{"gt_config"} containing all parameters.
#'
#' @examples
#' \dontrun{
#' gt <- ground_truth_config(
#'   df = read.csv("ground_truth.csv"),
#'   joining_schema = "clarity_omop_map",
#'   joining_person_table = "person_map",
#'   joining_visit_table = "visit_map",
#'   gt_person_key = "PatientDurableKey",
#'   omop_person_key = "PatientDurableKey",
#'   gt_encounter_key = "EncounterKey",
#'   omop_encounter_key = "EncounterKey",
#'   gt_icu_admission_col = "CrossIcuStayStartInstant",
#'   gt_icu_discharge_col = "CrossIcuStayEndInstant"
#' )
#'
#' data <- get_score_variables(..., visit_mode = "ground_truth", ground_truth = gt)
#' }
#'
#' @export
ground_truth_config <- function(df,
                                joining_schema,
                                joining_person_table,
                                joining_visit_table,
                                gt_person_key,
                                omop_person_key,
                                gt_encounter_key,
                                omop_encounter_key,
                                gt_icu_admission_col,
                                gt_icu_discharge_col,
                                gt_hospital_admission_col = NULL,
                                gt_hospital_discharge_col = NULL) {

  config <- list(
    df = df,
    joining_schema = joining_schema,
    joining_person_table = joining_person_table,
    joining_visit_table = joining_visit_table,
    gt_person_key = gt_person_key,
    omop_person_key = omop_person_key,
    gt_encounter_key = gt_encounter_key,
    omop_encounter_key = omop_encounter_key,
    gt_icu_admission_col = gt_icu_admission_col,
    gt_icu_discharge_col = gt_icu_discharge_col,
    gt_hospital_admission_col = gt_hospital_admission_col,
    gt_hospital_discharge_col = gt_hospital_discharge_col
  )
  class(config) <- "gt_config"
  config
}


#' Format a datetime column as a character string safe for SQL.
#'
#' Handles POSIXct, POSIXlt, Date, and character inputs without any timezone
#' conversion. If the input is already POSIXct/POSIXlt, \code{format()} is
#' called without specifying a \code{tz} argument, so the object's own
#' timezone attribute is preserved. If the input is character, it is returned
#' as-is (the database will parse it).
#'
#' @param x A vector of datetimes (POSIXct, POSIXlt, Date, or character).
#'
#' @return A character vector of datetime strings in \code{YYYY-MM-DD HH:MM:SS}
#'   format, or the original character strings if the input was already character.
format_datetime_for_sql <- function(x) {
  if (inherits(x, "POSIXct") || inherits(x, "POSIXlt")) {
    # format() without tz= uses the object's own timezone — no conversion.
    format(x, "%Y-%m-%d %H:%M:%S")
  } else if (inherits(x, "Date")) {
    format(x, "%Y-%m-%d 00:00:00")
  } else if (is.character(x)) {
    # Already character — pass through. The database will parse it.
    x
  } else {
    # Fallback: coerce to character.
    warning("Datetime column is not POSIXct, Date, or character. ",
            "Coercing to character — check that the format is correct.")
    as.character(x)
  }
}


#' Validate a ground truth dataframe before use.
#'
#' Checks that required columns exist, that key columns have no NAs,
#' and that ICU admission is not after ICU discharge. Stops with an informative
#' error if validation fails.
#'
#' @param gt_config A \code{gt_config} object created by \code{ground_truth_config}.
#'
#' @return The gt_config object, invisibly. Called for its side effects (stopping on error).
#'
#' @importFrom glue glue
validate_ground_truth <- function(gt_config) {

  ground_truth_df <- gt_config$df
  gt_person_key <- gt_config$gt_person_key
  gt_encounter_key <- gt_config$gt_encounter_key
  gt_icu_admission_col <- gt_config$gt_icu_admission_col
  gt_icu_discharge_col <- gt_config$gt_icu_discharge_col
  gt_hospital_admission_col <- gt_config$gt_hospital_admission_col
  gt_hospital_discharge_col <- gt_config$gt_hospital_discharge_col

  # Check required columns exist
  required_cols <- c(gt_person_key, gt_encounter_key,
                     gt_icu_admission_col, gt_icu_discharge_col)

  if (!is.null(gt_hospital_admission_col)) {
    required_cols <- c(required_cols, gt_hospital_admission_col)
  }
  if (!is.null(gt_hospital_discharge_col)) {
    required_cols <- c(required_cols, gt_hospital_discharge_col)
  }

  missing_cols <- setdiff(required_cols, colnames(ground_truth_df))
  if (length(missing_cols) > 0) {
    stop(glue("Ground truth dataframe is missing required columns: ",
              paste(missing_cols, collapse = ", ")))
  }

  # Check for NA in key columns
  for (col in c(gt_person_key, gt_encounter_key,
                gt_icu_admission_col, gt_icu_discharge_col)) {
    if (any(is.na(ground_truth_df[[col]]))) {
      stop(glue("Column '{col}' in ground truth contains NA values."))
    }
  }

  # Check ICU admission is before discharge
  # Compare as character to avoid timezone issues — lexicographic comparison
  # works for ISO-format datetime strings.
  icu_adm_str <- format_datetime_for_sql(ground_truth_df[[gt_icu_admission_col]])
  icu_dis_str <- format_datetime_for_sql(ground_truth_df[[gt_icu_discharge_col]])
  bad_rows <- which(icu_adm_str > icu_dis_str)
  if (length(bad_rows) > 0) {
    stop(glue("Ground truth has {length(bad_rows)} row(s) where ICU admission is ",
              "after ICU discharge. First bad row index: {bad_rows[1]}"))
  }

  # Check hospital times if provided
  if (!is.null(gt_hospital_admission_col) && !is.null(gt_hospital_discharge_col)) {
    hosp_adm_str <- format_datetime_for_sql(ground_truth_df[[gt_hospital_admission_col]])
    hosp_dis_str <- format_datetime_for_sql(ground_truth_df[[gt_hospital_discharge_col]])
    bad_hosp <- which(hosp_adm_str > hosp_dis_str)
    if (length(bad_hosp) > 0) {
      warning(glue("Ground truth has {length(bad_hosp)} row(s) where hospital admission ",
                   "is after hospital discharge. First bad row index: {bad_hosp[1]}"))
    }
  }

  # Check for duplicate rows per (person_key, icu_admission, icu_discharge).
  # Multiple rows per CrossICU stay (e.g. from bed moves) will cause duplicate
  # VALUES in the SQL query and corrupt per-stay aggregated columns when joining
  # back. The ground truth should be collapsed to one row per stay before use.
  dup_key_cols <- c(gt_person_key, gt_icu_admission_col, gt_icu_discharge_col)
  dup_counts <- as.data.frame(table(
    do.call(paste, c(ground_truth_df[dup_key_cols], sep = "|"))))
  n_dup_keys <- sum(dup_counts$Freq > 1)
  if (n_dup_keys > 0) {
    total_extra <- sum(dup_counts$Freq[dup_counts$Freq > 1]) - n_dup_keys
    warning(glue("Ground truth has {n_dup_keys} unique (patient, ICU admission, ",
                 "ICU discharge) combination(s) with multiple rows ",
                 "({total_extra} extra row(s) total). ",
                 "This will cause duplicate VALUES in the SQL query. ",
                 "Collapse to one row per ICU stay before use ",
                 "(e.g. aggregate in the source query)."))
  }

  invisible(gt_config)
}


#' Check overlap between ground truth patients and OMOP patients.
#'
#' Performs a bidirectional check:
#' \enumerate{
#'   \item Which ground truth patients are missing from OMOP (via the joining schema)?
#'   \item Which OMOP ICU patients (from visit_detail) are missing from ground truth?
#' }
#'
#' Prints warnings summarising mismatches and returns a list with the details.
#'
#' @param conn A database connection object.
#' @param gt_config A \code{gt_config} object created by \code{ground_truth_config}.
#' @param schema The OMOP schema name.
#' @param start_date Start date filter for OMOP ICU stays (character, inclusive).
#' @param end_date End date filter for OMOP ICU stays (character, inclusive).
#' @param dialect A SQL dialect supported by SqlRender.
#'
#' @return A list with two elements:
#' \describe{
#'   \item{gt_missing_from_omop}{Vector of ground truth patient keys not found in OMOP.}
#'   \item{omop_missing_from_gt}{Dataframe of OMOP person_ids with ICU stays not found in ground truth.}
#' }
#'
#' @importFrom DBI dbGetQuery
#' @importFrom glue glue
#' @importFrom SqlRender translate render
#' @export
validate_ground_truth_vs_omop <- function(conn,
                                          gt_config,
                                          schema,
                                          start_date,
                                          end_date,
                                          dialect) {

  ground_truth_df <- gt_config$df
  joining_schema <- gt_config$joining_schema
  joining_person_table <- gt_config$joining_person_table
  gt_person_key <- gt_config$gt_person_key
  omop_person_key <- gt_config$omop_person_key

  # Quote column names for safe use in SQL (PostgreSQL lowercases unquoted identifiers)
  qpk <- quote_identifier(omop_person_key, dialect)

  # --- Check 1: Which GT patients are missing from OMOP? ---
  gt_patient_keys <- unique(ground_truth_df[[gt_person_key]])

  batch_size <- 5000
  batches <- split(gt_patient_keys,
                   ceiling(seq_along(gt_patient_keys) / batch_size))

  found_keys <- c()
  for (batch in batches) {
    if (is.numeric(batch)) {
      key_list <- paste(batch, collapse = ", ")
    } else {
      key_list <- paste0("'", batch, "'", collapse = ", ")
    }

    sql <- glue("SELECT DISTINCT {qpk}
                 FROM {joining_schema}.{joining_person_table}
                 WHERE {qpk} IN ({key_list})")
    sql <- translate(sql, tolower(dialect))
    result <- dbGetQuery(conn, sql)
    colnames(result)[1] <- omop_person_key
    found_keys <- c(found_keys, result[[omop_person_key]])
  }

  gt_missing <- setdiff(as.character(gt_patient_keys),
                        as.character(found_keys))

  if (length(gt_missing) > 0) {
    warning(glue("{length(gt_missing)} ground truth patient(s) not found in OMOP ",
                 "joining schema ({joining_schema}.{joining_person_table}). ",
                 "First 10: {paste(head(gt_missing, 10), collapse = ', ')}"))
  } else {
    message("All ground truth patients found in OMOP joining schema.")
  }

  # --- Check 2: Which OMOP ICU patients are missing from GT? ---
  omop_icu_sql <- glue("
    SELECT DISTINCT jp.{qpk}, vd.person_id
    FROM {schema}.visit_detail vd
    INNER JOIN {joining_schema}.{joining_person_table} jp
      ON vd.person_id = jp.person_id
    WHERE vd.visit_detail_concept_id IN (581379, 32037)
      AND COALESCE(vd.visit_detail_start_datetime, vd.visit_detail_start_date)
          >= '{start_date}'
      AND COALESCE(vd.visit_detail_start_datetime, vd.visit_detail_start_date)
          <= '{end_date}'")
  omop_icu_sql <- translate(omop_icu_sql, tolower(dialect))
  omop_icu_patients <- dbGetQuery(conn, omop_icu_sql)

  # Normalise column name from DB result
  if (ncol(omop_icu_patients) >= 1) colnames(omop_icu_patients)[1] <- omop_person_key

  if (nrow(omop_icu_patients) > 0) {
    omop_missing <- omop_icu_patients[
      !as.character(omop_icu_patients[[omop_person_key]]) %in%
        as.character(gt_patient_keys), ]

    if (nrow(omop_missing) > 0) {
      warning(glue("{nrow(omop_missing)} OMOP ICU patient(s) in date range not found ",
                   "in ground truth. ",
                   "First 10 person_ids: ",
                   "{paste(head(omop_missing$person_id, 10), collapse = ', ')}"))
    } else {
      message("All OMOP ICU patients in date range found in ground truth.")
    }
  } else {
    warning("No OMOP ICU patients found in the specified date range.")
    omop_missing <- data.frame()
  }

  list(
    gt_missing_from_omop = gt_missing,
    omop_missing_from_gt = omop_missing
  )
}


#' Resolve ground truth to OMOP IDs.
#'
#' Queries the joining schema to map external patient and encounter keys
#' to OMOP person_id and visit_occurrence_id. Returns the full ground truth
#' dataframe with resolved IDs attached. Rows that cannot be resolved are
#' dropped with a warning.
#'
#' @param conn A database connection object.
#' @param gt_config A \code{gt_config} object created by \code{ground_truth_config}.
#' @param dialect A SQL dialect supported by SqlRender.
#'
#' @return The ground truth dataframe with \code{person_id},
#'   \code{visit_occurrence_id}, and \code{icu_admission_datetime} /
#'   \code{icu_discharge_datetime} (as formatted character strings) added.
#'   All original columns are preserved.
#'
#' @importFrom DBI dbGetQuery dbExecute
#' @importFrom glue glue glue_collapse
#' @importFrom SqlRender translate
resolve_ground_truth_ids <- function(conn, gt_config, dialect) {

  ground_truth_df    <- gt_config$df
  joining_schema     <- gt_config$joining_schema
  joining_person_table  <- gt_config$joining_person_table
  joining_visit_table   <- gt_config$joining_visit_table
  gt_person_key      <- gt_config$gt_person_key
  omop_person_key    <- gt_config$omop_person_key
  gt_encounter_key   <- gt_config$gt_encounter_key
  omop_encounter_key <- gt_config$omop_encounter_key
  gt_icu_admission_col  <- gt_config$gt_icu_admission_col
  gt_icu_discharge_col  <- gt_config$gt_icu_discharge_col

  # Quote column names for SQL (PostgreSQL lowercases unquoted identifiers)
  qpk <- quote_identifier(omop_person_key, dialect)
  qek <- quote_identifier(omop_encounter_key, dialect)

  # --- Resolve person keys to person_id ---
  # Upload GT keys to temp tables and join server-side.
  # This is much faster than IN (...) against large unindexed joining tables.
  gt_patient_keys <- unique(ground_truth_df[[gt_person_key]])
  is_numeric_pk <- is.numeric(gt_patient_keys)

  dbExecute(conn, "DROP TABLE IF EXISTS gt_person_keys_temp")
  col_type <- if (is_numeric_pk) "BIGINT" else "TEXT"
  dbExecute(conn, glue("CREATE TEMP TABLE gt_person_keys_temp (key_val {col_type})"))
  batch_sz <- 5000
  for (start in seq(1, length(gt_patient_keys), by = batch_sz)) {
    end <- min(start + batch_sz - 1, length(gt_patient_keys))
    chunk <- gt_patient_keys[start:end]
    if (is_numeric_pk) {
      vals <- glue_collapse(glue("({chunk})"), sep = ", ")
    } else {
      vals <- glue_collapse(glue("('{chunk}')"), sep = ", ")
    }
    dbExecute(conn, glue("INSERT INTO gt_person_keys_temp VALUES {vals}"))
  }
  dbExecute(conn, "ANALYZE gt_person_keys_temp")

  person_sql <- glue("SELECT DISTINCT j.{qpk}, j.person_id
                      FROM {joining_schema}.{joining_person_table} j
                      INNER JOIN gt_person_keys_temp k
                        ON j.{qpk} = k.key_val")
  person_sql <- translate(person_sql, tolower(dialect))
  person_map <- dbGetQuery(conn, person_sql)
  dbExecute(conn, "DROP TABLE IF EXISTS gt_person_keys_temp")

  # --- Resolve encounter keys to visit_occurrence_id ---
  gt_encounter_keys <- unique(ground_truth_df[[gt_encounter_key]])
  is_numeric_ek <- is.numeric(gt_encounter_keys)

  dbExecute(conn, "DROP TABLE IF EXISTS gt_encounter_keys_temp")
  col_type <- if (is_numeric_ek) "BIGINT" else "TEXT"
  dbExecute(conn, glue("CREATE TEMP TABLE gt_encounter_keys_temp (key_val {col_type})"))
  for (start in seq(1, length(gt_encounter_keys), by = batch_sz)) {
    end <- min(start + batch_sz - 1, length(gt_encounter_keys))
    chunk <- gt_encounter_keys[start:end]
    if (is_numeric_ek) {
      vals <- glue_collapse(glue("({chunk})"), sep = ", ")
    } else {
      vals <- glue_collapse(glue("('{chunk}')"), sep = ", ")
    }
    dbExecute(conn, glue("INSERT INTO gt_encounter_keys_temp VALUES {vals}"))
  }
  dbExecute(conn, "ANALYZE gt_encounter_keys_temp")

  visit_sql <- glue("SELECT DISTINCT j.{qek}, j.visit_occurrence_id
                     FROM {joining_schema}.{joining_visit_table} j
                     INNER JOIN gt_encounter_keys_temp k
                       ON j.{qek} = k.key_val")
  visit_sql <- translate(visit_sql, tolower(dialect))
  visit_map <- dbGetQuery(conn, visit_sql)
  dbExecute(conn, "DROP TABLE IF EXISTS gt_encounter_keys_temp")

  # Normalise column names: the DB may return the quoted column name in a
  # different case than expected (e.g. PostgreSQL may lowercase it despite
  # quoting, depending on driver version). Force the first column to match
  # the expected key name so merge() works.
  if (ncol(person_map) >= 1) colnames(person_map)[1] <- omop_person_key
  if (ncol(visit_map) >= 1)  colnames(visit_map)[1]  <- omop_encounter_key

  # --- Join mappings to ground truth ---
  resolved <- merge(
    ground_truth_df,
    person_map,
    by.x = gt_person_key,
    by.y = omop_person_key,
    all.x = TRUE
  )

  resolved <- merge(
    resolved,
    visit_map,
    by.x = gt_encounter_key,
    by.y = omop_encounter_key,
    all.x = TRUE
  )

  # Check for unresolved rows
  unresolved_person <- is.na(resolved$person_id)
  unresolved_visit <- is.na(resolved$visit_occurrence_id)
  unresolved <- unresolved_person | unresolved_visit

  if (any(unresolved)) {
    n_bad <- sum(unresolved)
    warning(glue("{n_bad} ground truth row(s) could not be resolved to OMOP IDs ",
                 "and will be dropped. ",
                 "({sum(unresolved_person)} missing person_id, ",
                 "{sum(unresolved_visit)} missing visit_occurrence_id)"))
    resolved <- resolved[!unresolved, ]
  }

  if (nrow(resolved) == 0) {
    stop("No ground truth rows could be resolved to OMOP IDs. ",
         "Check that the joining schema and key columns are correct.")
  }

  message(glue("{nrow(resolved)} ground truth row(s) resolved to OMOP IDs ",
               "({length(unique(resolved$person_id))} unique patients)."))

  # Add standardised datetime columns as formatted character strings.
  # Using format_datetime_for_sql to avoid any timezone conversion.
  resolved$icu_admission_datetime <- format_datetime_for_sql(
    resolved[[gt_icu_admission_col]])
  resolved$icu_discharge_datetime <- format_datetime_for_sql(
    resolved[[gt_icu_discharge_col]])

  resolved
}


#' Build a SQL VALUES clause from a resolved ground truth dataframe.
#'
#' Takes a resolved ground truth dataframe (as returned by
#' \code{resolve_ground_truth_ids}), filters to a batch of person_ids,
#' and returns a SQL VALUES string.
#'
#' @param resolved_gt Dataframe as returned by \code{resolve_ground_truth_ids}.
#' @param person_ids_batch Vector of person_ids for this batch.
#' @param dialect SQL dialect, used for correct timestamp cast syntax.
#'
#' @return A character string like
#'   \code{(1, 100, CAST('2022-07-01 10:00:00' AS TIMESTAMP), ...)}.
#'   Suitable for substitution into \code{@ground_truth_values} in the SQL query.
#'
#' @importFrom glue glue glue_collapse
#'
#' @note The VALUES clause is inlined into the SQL query, so its size scales
#'   with the number of ground truth rows per batch. With the default
#'   \code{batch_size = 10000}, a batch could produce a ~500KB SQL string.
#'   This works but is not elegant. If performance is a concern, reduce
#'   \code{batch_size} in \code{get_score_variables}.
build_ground_truth_values_clause <- function(resolved_gt, person_ids_batch, dialect) {

  batch_rows <- resolved_gt[resolved_gt$person_id %in% person_ids_batch, ]

  # Dialect-appropriate timestamp type
  ts_type <- if (tolower(dialect) == "postgresql") "TIMESTAMP" else "DATETIME"

  if (nrow(batch_rows) == 0) {
    return(glue("(0, 0, CAST('1900-01-01' AS {ts_type}), CAST('1900-01-01' AS {ts_type}))"))
  }

  # icu_admission_datetime and icu_discharge_datetime are already formatted
  # as character strings by resolve_ground_truth_ids, so no format() call needed here.
  rows <- glue_collapse(
    glue("({batch_rows$person_id}, ",
         "{batch_rows$visit_occurrence_id}, ",
         "CAST('{batch_rows$icu_admission_datetime}' AS {ts_type}), ",
         "CAST('{batch_rows$icu_discharge_datetime}' AS {ts_type}))"),
    sep = ",\n    "
  )

  as.character(rows)
}
