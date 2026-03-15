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

    sql <- glue("SELECT DISTINCT {omop_person_key}
                 FROM {joining_schema}.{joining_person_table}
                 WHERE {omop_person_key} IN ({key_list})")
    sql <- translate(sql, tolower(dialect))
    result <- dbGetQuery(conn, sql)
    found_keys <- c(found_keys, result[[1]])
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
    SELECT DISTINCT jp.{omop_person_key}, vd.person_id
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
#' @importFrom DBI dbGetQuery
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

  # --- Resolve person keys to person_id ---
  gt_patient_keys <- unique(ground_truth_df[[gt_person_key]])

  if (is.numeric(gt_patient_keys)) {
    key_list <- glue_collapse(gt_patient_keys, sep = ", ")
  } else {
    key_list <- glue_collapse(
      paste0("'", gt_patient_keys, "'"), sep = ", ")
  }

  person_sql <- glue("SELECT DISTINCT {omop_person_key}, person_id
                      FROM {joining_schema}.{joining_person_table}
                      WHERE {omop_person_key} IN ({key_list})")
  person_sql <- translate(person_sql, tolower(dialect))
  person_map <- dbGetQuery(conn, person_sql)

  # --- Resolve encounter keys to visit_occurrence_id ---
  gt_encounter_keys <- unique(ground_truth_df[[gt_encounter_key]])

  if (is.numeric(gt_encounter_keys)) {
    enc_list <- glue_collapse(gt_encounter_keys, sep = ", ")
  } else {
    enc_list <- glue_collapse(
      paste0("'", gt_encounter_keys, "'"), sep = ", ")
  }

  visit_sql <- glue("SELECT DISTINCT {omop_encounter_key}, visit_occurrence_id
                     FROM {joining_schema}.{joining_visit_table}
                     WHERE {omop_encounter_key} IN ({enc_list})")
  visit_sql <- translate(visit_sql, tolower(dialect))
  visit_map <- dbGetQuery(conn, visit_sql)

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
