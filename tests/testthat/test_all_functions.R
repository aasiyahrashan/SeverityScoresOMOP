# Tests for SeverityScoresOMOP
# Run with: testthat::test_file("tests/testthat/test-all-functions.R")
#
# These tests cover all functions that can be tested without a database connection.
# Functions requiring a live DB (get_score_variables, resolve_ground_truth_ids,
# validate_ground_truth_vs_omop, resolve_string_searches) are not tested here.

library(testthat)
library(dplyr)
library(glue)
library(data.table)

# =============================================================================
# ground_truth_config
# =============================================================================
test_that("ground_truth_config creates correct structure", {
  df <- data.frame(
    PatientDurableKey = 1:3,
    EncounterKey = 101:103,
    icu_adm = as.POSIXct(c("2022-07-01", "2022-07-02", "2022-07-03")),
    icu_dis = as.POSIXct(c("2022-07-05", "2022-07-06", "2022-07-07"))
  )

  gt <- ground_truth_config(
    df = df,
    joining_schema = "map_schema",
    joining_person_table = "person_map",
    joining_visit_table = "visit_map",
    gt_person_key = "PatientDurableKey",
    omop_person_key = "PatientDurableKey",
    gt_encounter_key = "EncounterKey",
    omop_encounter_key = "EncounterKey",
    gt_icu_admission_col = "icu_adm",
    gt_icu_discharge_col = "icu_dis"
  )

  expect_s3_class(gt, "gt_config")
  expect_identical(gt$joining_schema, "map_schema")
  expect_identical(gt$gt_person_key, "PatientDurableKey")
  expect_null(gt$gt_hospital_admission_col)
  expect_null(gt$gt_hospital_discharge_col)
  expect_equal(nrow(gt$df), 3)
})

test_that("ground_truth_config stores optional hospital cols", {
  df <- data.frame(
    pid = 1, enc = 1,
    icu_adm = "2022-07-01", icu_dis = "2022-07-05",
    hosp_adm = "2022-06-30", hosp_dis = "2022-07-10"
  )

  gt <- ground_truth_config(
    df = df, joining_schema = "s", joining_person_table = "p",
    joining_visit_table = "v", gt_person_key = "pid", omop_person_key = "pid",
    gt_encounter_key = "enc", omop_encounter_key = "enc",
    gt_icu_admission_col = "icu_adm", gt_icu_discharge_col = "icu_dis",
    gt_hospital_admission_col = "hosp_adm",
    gt_hospital_discharge_col = "hosp_dis"
  )

  expect_identical(gt$gt_hospital_admission_col, "hosp_adm")
  expect_identical(gt$gt_hospital_discharge_col, "hosp_dis")
})


# =============================================================================
# validate_ground_truth
# =============================================================================
make_gt_config <- function(df, icu_adm = "icu_adm", icu_dis = "icu_dis",
                           hosp_adm = NULL, hosp_dis = NULL) {
  ground_truth_config(
    df = df, joining_schema = "s", joining_person_table = "p",
    joining_visit_table = "v", gt_person_key = "pid", omop_person_key = "pid",
    gt_encounter_key = "enc", omop_encounter_key = "enc",
    gt_icu_admission_col = icu_adm, gt_icu_discharge_col = icu_dis,
    gt_hospital_admission_col = hosp_adm,
    gt_hospital_discharge_col = hosp_dis
  )
}

test_that("validate_ground_truth passes on valid data", {
  df <- data.frame(
    pid = 1:3, enc = 101:103,
    icu_adm = as.POSIXct(c("2022-07-01", "2022-07-02", "2022-07-03")),
    icu_dis = as.POSIXct(c("2022-07-05", "2022-07-06", "2022-07-07"))
  )
  gt <- make_gt_config(df)
  expect_silent(validate_ground_truth(gt))
})

test_that("validate_ground_truth stops on missing columns", {
  df <- data.frame(pid = 1, enc = 1, icu_adm = "2022-07-01")
  gt <- make_gt_config(df)
  expect_error(validate_ground_truth(gt), "missing required columns")
})

test_that("validate_ground_truth stops on NA in person key", {
  df <- data.frame(
    pid = c(1, NA), enc = 1:2,
    icu_adm = c("2022-07-01", "2022-07-02"),
    icu_dis = c("2022-07-05", "2022-07-06")
  )
  gt <- make_gt_config(df)
  expect_error(validate_ground_truth(gt), "contains NA values")
})

test_that("validate_ground_truth stops on NA in encounter key", {
  df <- data.frame(
    pid = 1:2, enc = c(1, NA),
    icu_adm = c("2022-07-01", "2022-07-02"),
    icu_dis = c("2022-07-05", "2022-07-06")
  )
  gt <- make_gt_config(df)
  expect_error(validate_ground_truth(gt), "contains NA values")
})

test_that("validate_ground_truth stops on NA in ICU admission", {
  df <- data.frame(
    pid = 1:2, enc = 1:2,
    icu_adm = c("2022-07-01", NA),
    icu_dis = c("2022-07-05", "2022-07-06")
  )
  gt <- make_gt_config(df)
  expect_error(validate_ground_truth(gt), "contains NA values")
})

test_that("validate_ground_truth stops when admission after discharge", {
  df <- data.frame(
    pid = 1, enc = 1,
    icu_adm = as.POSIXct("2022-07-10"),
    icu_dis = as.POSIXct("2022-07-05")
  )
  gt <- make_gt_config(df)
  expect_error(validate_ground_truth(gt), "admission is after ICU discharge")
})

test_that("validate_ground_truth warns on hospital admission after discharge", {
  df <- data.frame(
    pid = 1, enc = 1,
    icu_adm = as.POSIXct("2022-07-01"),
    icu_dis = as.POSIXct("2022-07-05"),
    hosp_adm = as.POSIXct("2022-07-20"),
    hosp_dis = as.POSIXct("2022-07-10")
  )
  gt <- make_gt_config(df, hosp_adm = "hosp_adm", hosp_dis = "hosp_dis")
  expect_warning(validate_ground_truth(gt), "hospital admission")
})

test_that("validate_ground_truth works with character datetime columns", {
  df <- data.frame(
    pid = 1, enc = 1,
    icu_adm = "2022-07-01 10:00:00",
    icu_dis = "2022-07-05 14:00:00"
  )
  gt <- make_gt_config(df)
  expect_silent(validate_ground_truth(gt))
})


# =============================================================================
# format_datetime_for_sql
# =============================================================================
test_that("format_datetime_for_sql handles POSIXct", {
  x <- as.POSIXct("2022-07-01 10:30:00", tz = "UTC")
  result <- format_datetime_for_sql(x)
  expect_equal(result, "2022-07-01 10:30:00")
})

test_that("format_datetime_for_sql handles Date", {
  x <- as.Date("2022-07-01")
  result <- format_datetime_for_sql(x)
  expect_equal(result, "2022-07-01 00:00:00")
})

test_that("format_datetime_for_sql passes through character", {
  x <- "2022-07-01 10:30:00"
  result <- format_datetime_for_sql(x)
  expect_equal(result, "2022-07-01 10:30:00")
})

test_that("format_datetime_for_sql warns on unknown type", {
  x <- 12345
  expect_warning(format_datetime_for_sql(x), "Coercing to character")
})

test_that("format_datetime_for_sql preserves POSIXct timezone", {
  x <- as.POSIXct("2022-07-01 10:30:00", tz = "America/New_York")
  result <- format_datetime_for_sql(x)
  expect_equal(result, "2022-07-01 10:30:00")
})

test_that("format_datetime_for_sql handles vector input", {
  x <- as.POSIXct(c("2022-07-01 10:00:00", "2022-07-02 14:30:00"), tz = "UTC")
  result <- format_datetime_for_sql(x)
  expect_length(result, 2)
  expect_equal(result[1], "2022-07-01 10:00:00")
  expect_equal(result[2], "2022-07-02 14:30:00")
})


# =============================================================================
# build_ground_truth_values_clause
# =============================================================================
test_that("build_ground_truth_values_clause produces valid SQL for postgresql", {
  resolved <- data.frame(
    person_id = c(1, 2),
    visit_occurrence_id = c(100, 200),
    icu_admission_datetime = c("2022-07-01 10:00:00", "2022-07-02 08:00:00"),
    icu_discharge_datetime = c("2022-07-05 14:00:00", "2022-07-06 12:00:00"),
    stringsAsFactors = FALSE
  )

  result <- build_ground_truth_values_clause(resolved, c(1, 2), "postgresql")

  expect_true(grepl("CAST.*AS TIMESTAMP", result))
  expect_true(grepl("2022-07-01 10:00:00", result))
  expect_true(grepl("2022-07-02 08:00:00", result))
  expect_equal(length(gregexpr("\\(\\d+,", result)[[1]]), 2)
})

test_that("build_ground_truth_values_clause uses DATETIME for sql server", {
  resolved <- data.frame(
    person_id = 1, visit_occurrence_id = 100,
    icu_admission_datetime = "2022-07-01 10:00:00",
    icu_discharge_datetime = "2022-07-05 14:00:00",
    stringsAsFactors = FALSE
  )

  result <- build_ground_truth_values_clause(resolved, 1, "sql server")
  expect_true(grepl("AS DATETIME", result))
  expect_false(grepl("AS TIMESTAMP", result))
})

test_that("build_ground_truth_values_clause filters by batch", {
  resolved <- data.frame(
    person_id = c(1, 2, 3),
    visit_occurrence_id = c(100, 200, 300),
    icu_admission_datetime = c("2022-07-01", "2022-07-02", "2022-07-03"),
    icu_discharge_datetime = c("2022-07-05", "2022-07-06", "2022-07-07"),
    stringsAsFactors = FALSE
  )

  result <- build_ground_truth_values_clause(resolved, 2, "postgresql")
  expect_true(grepl("\\(2,", result))
  expect_false(grepl("\\(1,", result))
  expect_false(grepl("\\(3,", result))
})

test_that("build_ground_truth_values_clause handles empty batch", {
  resolved <- data.frame(
    person_id = 1, visit_occurrence_id = 100,
    icu_admission_datetime = "2022-07-01",
    icu_discharge_datetime = "2022-07-05",
    stringsAsFactors = FALSE
  )

  result <- build_ground_truth_values_clause(resolved, 999, "postgresql")
  expect_true(grepl("1900-01-01", result))
})


# =============================================================================
# window_query
# =============================================================================
test_that("window_query returns DATEDIFF for calendar_date", {
  result <- window_query("calendar_date", "t.measurement_datetime",
                         "t.measurement_date", 24)
  expect_true(grepl("DATEDIFF", result))
  expect_true(grepl("dd", result))
})

test_that("window_query returns FLOOR for icu_admission_time", {
  result <- window_query("icu_admission_time", "t.measurement_datetime",
                         "t.measurement_date", 24)
  expect_true(grepl("FLOOR", result))
  expect_true(grepl("MINUTE", result))
  expect_true(grepl("DATEDIFF\\(MINUTE, adm\\.icu_admission_datetime, t\\.measurement_datetime\\)", result))
})

test_that("window_query stops on invalid window_start_point", {
  expect_error(window_query("invalid", "t.x", "t.y", 24),
               "window_start_point must be")
})

test_that("window_query stops if calendar_date with cadence != 24", {
  expect_error(window_query("calendar_date", "t.x", "t.y", 12),
               "cadence must be 24")
})

test_that("window_query stops on non-positive cadence", {
  expect_error(window_query("icu_admission_time", "t.x", "t.y", 0),
               "cadence must be")
  expect_error(window_query("icu_admission_time", "t.x", "t.y", -1),
               "cadence must be")
})


# =============================================================================
# age_query
# =============================================================================
test_that("age_query returns DATEDIFF for dob method", {
  result <- age_query("dob")
  expect_true(grepl("DATEDIFF", result))
  expect_true(grepl("birth_datetime", result))
})

test_that("age_query returns YEAR subtraction for year_only method", {
  result <- age_query("year_only")
  expect_true(grepl("year_of_birth", result))
  expect_false(grepl("DATEDIFF", result))
})

test_that("age_query stops on invalid method", {
  expect_error(age_query("invalid"), "age_method must be")
})


# =============================================================================
# variables_query (handles non-numeric / count variables only)
# =============================================================================
test_that("variables_query builds count for blank omop_variable", {
  concepts <- data.frame(
    short_name = "emergency_admission",
    omop_variable = NA_character_,
    concept_id = "12345",
    concept_id_value = NA,
    additional_filter_variable_name = NA,
    additional_filter_value = NA,
    stringsAsFactors = FALSE
  )

  result <- variables_query(concepts, "visit_detail_source_concept_id",
                            "visit_detail_id")
  expect_true(grepl("count_emergency_admission", result))
  expect_true(grepl("CASE WHEN", result))
})

test_that("variables_query builds count for value_as_concept_id", {
  concepts <- data.frame(
    short_name = "gcs_eye",
    omop_variable = "value_as_concept_id",
    concept_id = "3016335",
    concept_id_value = "45877537",
    additional_filter_variable_name = NA,
    additional_filter_value = NA,
    stringsAsFactors = FALSE
  )

  result <- variables_query(concepts, "measurement_concept_id",
                            "measurement_id")
  expect_true(grepl("count_gcs_eye", result))
  expect_true(grepl("value_as_concept_id IN", result))
})

test_that("variables_query builds count for ancestor_concept_id", {
  concepts <- data.frame(
    short_name = "ami",
    omop_variable = "ancestor_concept_id",
    concept_id = "45538370",
    concept_id_value = NA,
    additional_filter_variable_name = NA,
    additional_filter_value = NA,
    stringsAsFactors = FALSE
  )

  result <- variables_query(concepts, "condition_concept_id",
                            "condition_occurrence_id")
  expect_true(grepl("count_ami", result))
  expect_true(grepl("concept_ancestor", result))
})

test_that("variables_query returns empty string when no count concepts", {
  concepts <- data.frame(
    short_name = "hr",
    omop_variable = "value_as_number",
    concept_id = "4301868",
    concept_id_value = NA,
    additional_filter_variable_name = NA,
    additional_filter_value = NA,
    stringsAsFactors = FALSE
  )

  result <- variables_query(concepts, "measurement_concept_id",
                            "measurement_id")
  expect_equal(result, "")
})


# =============================================================================
# translate_drug_join
# =============================================================================
test_that("translate_drug_join returns LATERAL for postgresql", {
  result <- translate_drug_join("postgresql")
  expect_true(grepl("LEFT JOIN LATERAL", result))
  expect_true(grepl("ON TRUE", result))
})

test_that("translate_drug_join returns OUTER APPLY for sql server", {
  result <- translate_drug_join("sql server")
  expect_true(grepl("OUTER APPLY", result))
})

test_that("translate_drug_join stops on unsupported dialect", {
  expect_error(translate_drug_join("sqlite"), "Unsupported dialect")
})


# =============================================================================
# build_concept_map
# =============================================================================
test_that("build_concept_map creates mapping for numeric concepts", {
  concepts <- data.frame(
    concept_id = c("4301868", "4108446"),
    short_name = c("hr", "rr"),
    omop_variable = c("value_as_number", "value_as_number"),
    additional_filter_variable_name = c(NA, "measurement_source_value"),
    additional_filter_value = c(NA, "R VENT RESP RATE OBSERVED"),
    stringsAsFactors = FALSE
  )

  result <- build_concept_map(concepts)
  expect_s3_class(result, "data.table")
  expect_true("concept_id" %in% names(result))
  expect_true("short_name" %in% names(result))
  expect_equal(nrow(result), 2)
  expect_equal(result[concept_id == "4301868"]$short_name, "hr")
})

test_that("build_concept_map returns empty for no numeric concepts", {
  concepts <- data.frame(
    concept_id = "12345",
    short_name = "comorbidity",
    omop_variable = NA_character_,
    additional_filter_variable_name = NA,
    additional_filter_value = NA,
    stringsAsFactors = FALSE
  )

  result <- build_concept_map(concepts)
  expect_equal(nrow(result), 0)
})


# =============================================================================
# build_filtered_temp
# =============================================================================
test_that("build_filtered_temp generates DROP, CREATE, ANALYZE", {
  concepts <- data.frame(
    table = "Measurement",
    short_name = "hr",
    concept_id = "4301868",
    omop_variable = "value_as_number",
    additional_filter_variable_name = NA,
    stringsAsFactors = FALSE
  )

  vn <- data.frame(
    table = "Measurement", db_table_name = "measurement", alias = "m",
    id_var = "measurement_id", concept_id_var = "measurement_concept_id",
    start_date_var = "measurement_date",
    start_datetime_var = "measurement_datetime",
    stringsAsFactors = FALSE
  )

  result <- build_filtered_temp(concepts, "Measurement", vn)
  expect_length(result, 3)
  expect_true(grepl("DROP TABLE", result[1]))
  expect_true(grepl("CREATE TEMP TABLE m_filtered_temp", result[2]))
  expect_true(grepl("ANALYZE", result[3]))
})

test_that("build_filtered_temp uses UNION when mixing direct IDs and ancestors", {
  concepts <- data.frame(
    table = rep("Condition", 2),
    short_name = c("comorbidity", "ami"),
    concept_id = c("443612", "45538370"),
    omop_variable = c(NA, "ancestor_concept_id"),
    additional_filter_variable_name = c(NA, NA),
    stringsAsFactors = FALSE
  )

  vn <- data.frame(
    table = "Condition", db_table_name = "condition_occurrence", alias = "co",
    id_var = "condition_occurrence_id",
    concept_id_var = "condition_concept_id",
    start_date_var = "condition_start_date",
    start_datetime_var = "condition_start_datetime",
    stringsAsFactors = FALSE
  )

  result <- build_filtered_temp(concepts, "Condition", vn)
  create_sql <- result[2]
  expect_true(grepl("UNION", create_sql))
  # Should NOT have OR between the two filter types
  expect_false(grepl("concept_ancestor.*OR.*IN \\(", create_sql))
})

test_that("build_filtered_temp uses simple WHERE for single filter type", {
  concepts <- data.frame(
    table = "Measurement",
    short_name = c("hr", "rr"),
    concept_id = c("4301868", "4108446"),
    omop_variable = c("value_as_number", "value_as_number"),
    additional_filter_variable_name = c(NA, NA),
    stringsAsFactors = FALSE
  )

  vn <- data.frame(
    table = "Measurement", db_table_name = "measurement", alias = "m",
    id_var = "measurement_id", concept_id_var = "measurement_concept_id",
    start_date_var = "measurement_date",
    start_datetime_var = "measurement_datetime",
    stringsAsFactors = FALSE
  )

  result <- build_filtered_temp(concepts, "Measurement", vn)
  create_sql <- result[2]
  expect_false(grepl("UNION", create_sql))
  expect_true(grepl("IN \\(4301868, 4108446\\)", create_sql))
})

test_that("build_filtered_temp selects unit_concept_id but does NOT join concept for unit name", {
  concepts <- data.frame(
    table = "Measurement",
    short_name = "hr",
    concept_id = "4301868",
    omop_variable = "value_as_number",
    additional_filter_variable_name = NA,
    stringsAsFactors = FALSE
  )

  vn <- data.frame(
    table = "Measurement", db_table_name = "measurement", alias = "m",
    id_var = "measurement_id", concept_id_var = "measurement_concept_id",
    start_date_var = "measurement_date",
    start_datetime_var = "measurement_datetime",
    stringsAsFactors = FALSE
  )

  result <- build_filtered_temp(concepts, "Measurement", vn)
  expect_true(grepl("unit_concept_id", result[2]))
  # unit name lookup now happens in R after pivoting, not via SQL JOIN
  expect_false(grepl("unit_name", result[2]))
  expect_false(grepl("LEFT JOIN.*concept", result[2]))
})


# =============================================================================
# mean_arterial_pressure
# =============================================================================
test_that("mean_arterial_pressure calculates from sbp and dbp", {
  data <- data.table(
    min_sbp = 120, max_sbp = 140,
    min_dbp = 80, max_dbp = 90
  )

  result <- mean_arterial_pressure(data)
  expect_equal(result$min_map, 80 + 1/3 * (120 - 80))
  expect_equal(result$max_map, 90 + 1/3 * (140 - 90))
})

test_that("mean_arterial_pressure does not overwrite existing map", {
  data <- data.table(
    min_map = 70, max_map = 100,
    min_sbp = 120, max_sbp = 140,
    min_dbp = 80, max_dbp = 90
  )

  result <- mean_arterial_pressure(data)
  expect_equal(result$min_map, 70)
  expect_equal(result$max_map, 100)
})


# =============================================================================
# total_gcs
# =============================================================================
test_that("total_gcs calculates from components", {
  data <- data.table(
    min_gcs_eye = 2, min_gcs_verbal = 3, min_gcs_motor = 4,
    max_gcs_eye = 4, max_gcs_verbal = 5, max_gcs_motor = 6
  )

  result <- total_gcs(data)
  expect_equal(result$min_gcs, 9)
  expect_equal(result$max_gcs, 15)
})

test_that("total_gcs does not overwrite existing gcs", {
  data <- data.table(
    min_gcs = 3, max_gcs = 15,
    min_gcs_eye = 1, min_gcs_verbal = 1, min_gcs_motor = 1,
    max_gcs_eye = 4, max_gcs_verbal = 5, max_gcs_motor = 6
  )

  result <- total_gcs(data)
  expect_equal(result$min_gcs, 3)
  expect_equal(result$max_gcs, 15)
})


# =============================================================================
# emergency_admission / renal_failure / comorbidities / mechanical_ventilation
# =============================================================================
test_that("emergency_admission defaults to 0", {
  data <- data.table(person_id = 1)
  result <- emergency_admission(data)
  expect_equal(result$emergency_admission, 0)
})

test_that("emergency_admission uses count if available", {
  data <- data.table(count_emergency_admission = c(0, 2))
  result <- emergency_admission(data)
  expect_equal(result$emergency_admission, c(0, 1))
})

test_that("renal_failure defaults to 0", {
  data <- data.table(person_id = 1)
  result <- renal_failure(data)
  expect_equal(result$renal_failure, 0)
})

test_that("comorbidities defaults to 0", {
  data <- data.table(person_id = 1)
  result <- comorbidities(data)
  expect_equal(result$comorbidity, 0)
})

test_that("mechanical_ventilation defaults to 0", {
  data <- data.table(person_id = 1)
  result <- mechanical_ventilation(data)
  expect_equal(result$mechanical_ventilation, 0)
})

test_that("mechanical_ventilation uses count if available", {
  data <- data.table(count_mechanical_ventilation = c(0, 3))
  result <- mechanical_ventilation(data)
  expect_equal(result$mechanical_ventilation, c(0, 1))
})


# =============================================================================
# APACHE II creatinine renal failure doubling
# =============================================================================
test_that("APACHE II creatinine score is doubled with renal failure", {
  data <- data.table(
    max_temp = 37, min_temp = 37,
    min_wcc = 10, max_wcc = 10,
    min_sbp = 120, max_sbp = 120,
    min_dbp = 80, max_dbp = 80,
    max_fio2 = 0.21, min_pao2 = 90, min_paco2 = 40,
    min_hematocrit = 40, max_hematocrit = 40,
    min_hr = 80, max_hr = 80,
    min_rr = 16, max_rr = 16,
    min_ph = 7.4, max_ph = 7.4,
    min_bicarbonate = 24, max_bicarbonate = 24,
    min_sodium = 140, max_sodium = 140,
    min_potassium = 4, max_potassium = 4,
    min_creatinine = 2.5, max_creatinine = 2.5,
    min_gcs = 15, max_gcs = 15,
    age = 50,
    count_renal_failure = 1,
    count_emergency_admission = 0
  )
  data[, unit_temp := "degree Celsius"]
  data[, unit_wcc := "billion per liter"]
  data[, unit_fio2 := "ratio"]
  data[, unit_pao2 := "millimeter mercury column"]
  data[, unit_paco2 := "millimeter mercury column"]
  data[, unit_hematocrit := "percent"]
  data[, unit_sodium := "millimole per liter"]
  data[, unit_potassium := "millimole per liter"]
  data[, unit_creatinine := "milligram per deciliter"]
  data[, unit_bicarbonate := "millimole per liter"]

  result <- calculate_apache_ii_score(data, imputation = "normal")

  result_no_rf <- copy(data)
  result_no_rf[, count_renal_failure := 0]
  result_no_rf <- calculate_apache_ii_score(result_no_rf, imputation = "normal")

  expect_equal(result$apache_ii_score - result_no_rf$apache_ii_score, 3)
})


# =============================================================================
# build_visit_sql
# =============================================================================
test_that("build_visit_sql returns SQL for paste mode", {
  result <- build_visit_sql("paste", "postgresql", paste_gap_hours = 6)
  expect_true(grepl("lagged_visit_details", result))
  expect_true(grepl("icu_admission_details", result))
})

test_that("build_visit_sql returns SQL for raw mode", {
  result <- build_visit_sql("raw", "postgresql")
  expect_true(grepl("icu_admission_details", result))
  expect_false(grepl("lagged_visit_details", result))
})

test_that("build_visit_sql returns SQL for ground_truth mode", {
  gt <- ground_truth_config(
    df = data.frame(pid = 1, enc = 1, adm = "2022-01-01", dis = "2022-01-05"),
    joining_schema = "s", joining_person_table = "p", joining_visit_table = "v",
    gt_person_key = "pid", omop_person_key = "pid",
    gt_encounter_key = "enc", omop_encounter_key = "enc",
    gt_icu_admission_col = "adm", gt_icu_discharge_col = "dis"
  )

  result <- build_visit_sql("ground_truth", "postgresql", ground_truth = gt)
  expect_true(grepl("ground_truth_data", result))
  expect_true(grepl("@ground_truth_values", result))
})


# =============================================================================
# check_required_columns
# =============================================================================
test_that("check_required_columns passes when all columns present", {
  df <- data.frame(a = 1, b = 2, c = 3)
  expect_silent(check_required_columns(df, c("a", "b"), "test_fn"))
})

test_that("check_required_columns stops on missing columns", {
  df <- data.frame(a = 1, b = 2)
  expect_error(
    check_required_columns(df, c("a", "c", "d"), "test_fn"),
    "test_fn: missing required column.*c.*d"
  )
})


# =============================================================================
# Scoring functions: column existence checks
# =============================================================================

test_that("calculate_apache_ii_score stops on missing columns", {
  data <- data.table(min_temp = 37)
  expect_error(calculate_apache_ii_score(data),
               "calculate_apache_ii_score: missing required column")
})


test_that("calculate_sofa_score stops on missing columns", {
  data <- data.table(min_platelet = 100)
  expect_error(calculate_sofa_score(data),
               "calculate_sofa_score: missing required column")
})


# =============================================================================
# normalise_concepts_columns
# =============================================================================
test_that("normalise_concepts_columns adds missing columns", {
  concepts <- data.frame(
    short_name = "hr", concept_id = "4301868",
    stringsAsFactors = FALSE
  )

  result <- normalise_concepts_columns(concepts)
  expect_true("concept_id_value" %in% names(result))
  expect_true("name_of_value" %in% names(result))
  expect_true("additional_filter_variable_name" %in% names(result))
  expect_true("additional_filter_value" %in% names(result))
})

test_that("normalise_concepts_columns maps afvv to afv", {
  concepts <- data.frame(
    short_name = "hr", concept_id = "4301868",
    additional_filter_variable_value = "test_val",
    stringsAsFactors = FALSE
  )

  result <- normalise_concepts_columns(concepts)
  expect_equal(result$additional_filter_value, "test_val")
})
