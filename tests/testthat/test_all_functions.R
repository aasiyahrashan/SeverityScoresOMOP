# Tests for SeverityScoresOMOP
# Run with: testthat::test_file("tests/testthat/test-all-functions.R")
#
# These tests cover all functions that can be tested without a database connection.
# Functions requiring a live DB (get_score_variables, resolve_ground_truth_ids,
# validate_ground_truth_vs_omop) are not tested here.

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
  # Missing icu_dis column
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
  # Create a time in a specific timezone
  x <- as.POSIXct("2022-07-01 10:30:00", tz = "America/New_York")
  result <- format_datetime_for_sql(x)
  # Should format in the object's timezone, not convert to UTC
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
  # Should have two rows
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

  # Only request person_id 2
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
  # Verify correct parenthesisation: DATEDIFF closes before the division
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
# where_clause
# =============================================================================
test_that("where_clause builds concept ID filter", {
  concepts <- data.frame(
    table = "Measurement",
    short_name = "hr",
    concept_id = "4301868",
    omop_variable = "value_as_number",
    concept_id_value = NA,
    additional_filter_variable_name = NA,
    additional_filter_value = NA,
    stringsAsFactors = FALSE
  )

  variable_names <- data.frame(
    table = "Measurement",
    db_table_name = "measurement",
    alias = "m",
    id_var = "measurement_id",
    concept_id_var = "measurement_concept_id",
    stringsAsFactors = FALSE
  )

  result <- where_clause(concepts, variable_names, "Measurement")
  expect_true(grepl("measurement_concept_id IN", result))
  expect_true(grepl("4301868", result))
})

test_that("where_clause returns false when no concepts match", {
  concepts <- data.frame(
    table = "Observation",
    short_name = "hr",
    concept_id = "4301868",
    omop_variable = "value_as_number",
    concept_id_value = NA,
    additional_filter_variable_name = NA,
    additional_filter_value = NA,
    stringsAsFactors = FALSE
  )

  variable_names <- data.frame(
    table = "Measurement",
    db_table_name = "measurement",
    alias = "m",
    id_var = "measurement_id",
    concept_id_var = "measurement_concept_id",
    stringsAsFactors = FALSE
  )

  # Asking for Measurement but concepts are in Observation
  result <- where_clause(concepts, variable_names, "Measurement")
  expect_equal(result, "false")
})


# =============================================================================
# variables_query
# =============================================================================
test_that("variables_query builds min/max/unit for numeric concepts", {
  concepts <- data.frame(
    short_name = "hr",
    omop_variable = "value_as_number",
    concept_id = "4301868",
    concept_id_value = NA,
    additional_filter_variable_name = NA,
    additional_filter_value = NA,
    stringsAsFactors = FALSE
  )

  result <- variables_query(concepts, "measurement_concept_id", "measurement_id")
  expect_true(grepl("max_hr", result))
  expect_true(grepl("min_hr", result))
  expect_true(grepl("unit_hr", result))
})

test_that("variables_query builds count for non-numeric concepts", {
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


# =============================================================================
# units_of_measure_query
# =============================================================================
test_that("units_of_measure_query returns join for Measurement", {
  result <- units_of_measure_query("Measurement")
  expect_true(grepl("LEFT JOIN", result))
  expect_true(grepl("unit_concept_id", result))
})

test_that("units_of_measure_query returns join for Observation", {
  result <- units_of_measure_query("Observation")
  expect_true(grepl("LEFT JOIN", result))
})

test_that("units_of_measure_query returns empty for other tables", {
  expect_equal(units_of_measure_query("Condition"), "")
  expect_equal(units_of_measure_query("Procedure"), "")
  expect_equal(units_of_measure_query("Drug"), "")
})


# =============================================================================
# all_required_variables_query
# =============================================================================
test_that("all_required_variables_query lists numeric vars correctly", {
  concepts <- data.frame(
    short_name = c("hr", "rr"),
    omop_variable = c("value_as_number", "value_as_number"),
    stringsAsFactors = FALSE
  )

  result <- all_required_variables_query(concepts)
  expect_true(grepl("min_hr", result))
  expect_true(grepl("max_hr", result))
  expect_true(grepl("unit_hr", result))
  expect_true(grepl("min_rr", result))
})

test_that("all_required_variables_query lists count vars correctly", {
  concepts <- data.frame(
    short_name = "emergency_admission",
    omop_variable = NA_character_,
    stringsAsFactors = FALSE
  )

  result <- all_required_variables_query(concepts)
  expect_true(grepl("count_emergency_admission", result))
})


# =============================================================================
# all_id_vars
# =============================================================================
test_that("all_id_vars produces correct coalesce string", {
  result <- all_id_vars(c("m", "o"), "person_id")
  expect_equal(result, "m.person_id, o.person_id")
})

test_that("all_id_vars works with single alias", {
  result <- all_id_vars("m", "time_in_icu")
  expect_equal(result, "m.time_in_icu")
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
  # MAP = DBP + 1/3 * (SBP - DBP)
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
  # Should not recalculate
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
# emergency_admission / renal_failure / comorbidities
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
  # Creatinine of 2.5 mg/dL should score 3 normally.
  # With renal failure, should be 6.
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
  # Add unit columns (required by the function)
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

  # Creatinine 2.5 mg/dL = subscore 3, doubled to 6 with renal failure.
  # Verify the score includes the doubled value by checking total is higher
  # than it would be without renal failure.
  result_no_rf <- copy(data)
  result_no_rf[, count_renal_failure := 0]
  result_no_rf <- calculate_apache_ii_score(result_no_rf, imputation = "normal")

  # The difference should be 3 (one creatinine subscore worth of doubling)
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
  # Should NOT have the grouping/lagging logic
  expect_false(grepl("lagged_visit_details", result))
  expect_false(grepl("group_id", result))
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
  expect_true(grepl("icu_admission_details", result))
})

test_that("build_visit_sql renders hospital cols as NULL when not provided", {
  gt <- ground_truth_config(
    df = data.frame(pid = 1, enc = 1, adm = "2022-01-01", dis = "2022-01-05"),
    joining_schema = "s", joining_person_table = "p", joining_visit_table = "v",
    gt_person_key = "pid", omop_person_key = "pid",
    gt_encounter_key = "enc", omop_encounter_key = "enc",
    gt_icu_admission_col = "adm", gt_icu_discharge_col = "dis"
  )

  result <- build_visit_sql("ground_truth", "postgresql", ground_truth = gt)
  # Should have COALESCE(NULL, ...) for hospital times
  expect_true(grepl("COALESCE\\(NULL", result))
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

test_that("check_required_columns error message names the calling function", {
  df <- data.frame(a = 1)
  expect_error(
    check_required_columns(df, c("b"), "fix_apache_ii_units"),
    "fix_apache_ii_units"
  )
})


# =============================================================================
# Scoring functions: column existence checks
# =============================================================================
test_that("fix_apache_ii_units stops on missing columns", {
  data <- data.table(min_temp = 37, max_temp = 38)
  expect_error(fix_apache_ii_units(data), "fix_apache_ii_units: missing required column")
})

test_that("fix_implausible_values_apache_ii stops on missing columns", {
  data <- data.table(min_temp = 37)
  expect_error(fix_implausible_values_apache_ii(data),
               "fix_implausible_values_apache_ii: missing required column")
})

test_that("calculate_apache_ii_score stops on missing columns", {
  data <- data.table(min_temp = 37)
  expect_error(calculate_apache_ii_score(data),
               "calculate_apache_ii_score: missing required column")
})

test_that("fix_sofa_units stops on missing columns", {
  data <- data.table(min_fio2 = 0.5)
  expect_error(fix_sofa_units(data), "fix_sofa_units: missing required column")
})

test_that("fix_implausible_values_sofa stops on missing columns", {
  data <- data.table(min_platelet = 100)
  expect_error(fix_implausible_values_sofa(data),
               "fix_implausible_values_sofa: missing required column")
})

test_that("calculate_sofa_score stops on missing columns", {
  data <- data.table(min_platelet = 100)
  expect_error(calculate_sofa_score(data),
               "calculate_sofa_score: missing required column")
})
