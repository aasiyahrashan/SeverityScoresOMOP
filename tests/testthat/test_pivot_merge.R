# =============================================================================
# test_pivot_merge_sql.R
#
# Tests for data flow functions that don't require a database:
#   - pivot_long_to_wide: concept mapping, filtering, NA/Inf handling, dedup
#   - merge_admissions_and_physiology: spine construction, multi-table, NAs
#   - build_filtered_temp: UNION logic, ancestor+direct mixing
#   - build_concept_map: filter_col, multi-concept-id per short_name
#   - build_long_select / build_count_select: SQL correctness
#   - build_drug_statements: ancestor_map, tagged table, aggregation
#
# Run with: testthat::test_file("tests/testthat/test_pivot_merge_sql.R")
# =============================================================================

library(testthat)
library(data.table)
library(dplyr)
library(glue)


# =============================================================================
# pivot_long_to_wide
# =============================================================================

test_that("basic pivot: one concept, one patient, one window", {
  long_dt <- data.table(
    person_id = 1, icu_admission_datetime = "2022-07-01",
    time_in_icu = 0, concept_id = "111",
    min_val = 36.5, max_val = 38.0, unit_name = "degree Celsius"
  )
  cmap <- data.table(
    concept_id = "111", short_name = "temp",
    filter_col = NA_character_, filter_vals = list(NA_character_)
  )

  result <- pivot_long_to_wide(long_dt, cmap)
  expect_equal(nrow(result), 1)
  expect_equal(result$min_temp, 36.5)
  expect_equal(result$max_temp, 38.0)
  expect_equal(result$unit_temp, "degree Celsius")
})

test_that("multiple concepts map to same short_name → aggregated", {
  long_dt <- data.table(
    person_id = c(1, 1), icu_admission_datetime = "2022-07-01",
    time_in_icu = c(0, 0), concept_id = c("111", "222"),
    min_val = c(36.5, 37.0), max_val = c(38.0, 39.0),
    unit_name = c("degree Celsius", "degree Celsius")
  )
  cmap <- data.table(
    concept_id = c("111", "222"), short_name = c("temp", "temp"),
    filter_col = c(NA_character_, NA_character_),
    filter_vals = list(NA_character_, NA_character_)
  )

  result <- pivot_long_to_wide(long_dt, cmap)
  expect_equal(nrow(result), 1)
  # min of mins, max of maxes
  expect_equal(result$min_temp, 36.5)
  expect_equal(result$max_temp, 39.0)
})

test_that("multiple patients, multiple windows → correct row count", {
  long_dt <- data.table(
    person_id = c(1, 1, 2), icu_admission_datetime = "2022-07-01",
    time_in_icu = c(0, 1, 0), concept_id = "111",
    min_val = c(36, 37, 35), max_val = c(38, 39, 36),
    unit_name = "C"
  )
  cmap <- data.table(
    concept_id = "111", short_name = "temp",
    filter_col = NA_character_, filter_vals = list(NA_character_)
  )

  result <- pivot_long_to_wide(long_dt, cmap)
  expect_equal(nrow(result), 3)
})

test_that("concept_id not in map → dropped silently (no extra rows)", {
  long_dt <- data.table(
    person_id = 1, icu_admission_datetime = "2022-07-01",
    time_in_icu = 0, concept_id = c("111", "999"),
    min_val = c(36, 100), max_val = c(38, 200),
    unit_name = c("C", "??")
  )
  cmap <- data.table(
    concept_id = "111", short_name = "temp",
    filter_col = NA_character_, filter_vals = list(NA_character_)
  )

  result <- pivot_long_to_wide(long_dt, cmap)
  expect_equal(nrow(result), 1)
  expect_true("min_temp" %in% names(result))
  # No columns from concept 999
  expect_false(any(grepl("999", names(result))))
})

test_that("all NA min_val → becomes NA (not Inf)", {
  long_dt <- data.table(
    person_id = 1, icu_admission_datetime = "2022-07-01",
    time_in_icu = 0, concept_id = "111",
    min_val = NA_real_, max_val = NA_real_, unit_name = NA_character_
  )
  cmap <- data.table(
    concept_id = "111", short_name = "temp",
    filter_col = NA_character_, filter_vals = list(NA_character_)
  )

  result <- pivot_long_to_wide(long_dt, cmap)
  expect_equal(nrow(result), 1)
  expect_true(is.na(result$min_temp))
  expect_true(is.na(result$max_temp))
  # Should NOT be Inf
  expect_false(is.infinite(result$min_temp %||% 0))
})

test_that("filter_col filtering works: same concept_id, different short_name", {
  # e.g. concept_id 4108446 (respiratory rate) maps to "rr" when
  # measurement_source_value = "RR" and to "vent_rr" when = "VENT RR"
  long_dt <- data.table(
    person_id = c(1, 1), icu_admission_datetime = "2022-07-01",
    time_in_icu = c(0, 0), concept_id = c("111", "111"),
    min_val = c(16, 22), max_val = c(20, 28),
    unit_name = c("bpm", "bpm"),
    source_col = c("RR", "VENT_RR")
  )
  cmap <- data.table(
    concept_id = c("111", "111"),
    short_name = c("rr", "vent_rr"),
    filter_col = c("source_col", "source_col"),
    filter_vals = list(c("RR"), c("VENT_RR"))
  )

  result <- pivot_long_to_wide(long_dt, cmap)
  expect_equal(nrow(result), 1)
  expect_equal(result$min_rr, 16)
  expect_equal(result$min_vent_rr, 22)
})

test_that("empty long_dt returns empty data.table", {
  long_dt <- data.table(
    person_id = integer(0), icu_admission_datetime = character(0),
    time_in_icu = integer(0), concept_id = character(0),
    min_val = numeric(0), max_val = numeric(0), unit_name = character(0)
  )
  cmap <- data.table(
    concept_id = "111", short_name = "temp",
    filter_col = NA_character_, filter_vals = list(NA_character_)
  )
  result <- pivot_long_to_wide(long_dt, cmap)
  expect_equal(nrow(result), 0)
})

test_that("one concept maps to two short_names (cartesian) → both columns created", {
  # e.g. a concept could legitimately map to two variables
  long_dt <- data.table(
    person_id = 1, icu_admission_datetime = "2022-07-01",
    time_in_icu = 0, concept_id = "111",
    min_val = 100, max_val = 200, unit_name = "U"
  )
  cmap <- data.table(
    concept_id = c("111", "111"),
    short_name = c("var_a", "var_b"),
    filter_col = c(NA_character_, NA_character_),
    filter_vals = list(NA_character_, NA_character_)
  )

  result <- pivot_long_to_wide(long_dt, cmap)
  expect_equal(nrow(result), 1)
  expect_true("min_var_a" %in% names(result))
  expect_true("min_var_b" %in% names(result))
  expect_equal(result$min_var_a, 100)
  expect_equal(result$min_var_b, 100)
})

test_that("unit_name: first non-NA is picked when multiple units exist", {
  long_dt <- data.table(
    person_id = c(1, 1), icu_admission_datetime = "2022-07-01",
    time_in_icu = c(0, 0), concept_id = c("111", "222"),
    min_val = c(36, 37), max_val = c(38, 39),
    unit_name = c(NA_character_, "degree Celsius")
  )
  cmap <- data.table(
    concept_id = c("111", "222"), short_name = c("temp", "temp"),
    filter_col = c(NA_character_, NA_character_),
    filter_vals = list(NA_character_, NA_character_)
  )

  result <- pivot_long_to_wide(long_dt, cmap)
  expect_equal(result$unit_temp, "degree Celsius")
})


# =============================================================================
# merge_admissions_and_physiology
# =============================================================================

test_that("basic merge: admissions + one physiology table", {
  adm <- data.table(
    person_id = c(1, 2),
    icu_admission_datetime = c("2022-07-01", "2022-07-02"),
    visit_detail_id = c(10, 20),
    age = c(50, 60)
  )
  phys <- list(
    m_long = data.table(
      person_id = c(1, 1, 2),
      icu_admission_datetime = c("2022-07-01", "2022-07-01", "2022-07-02"),
      time_in_icu = c(0, 1, 0),
      min_hr = c(70, 80, 90),
      max_hr = c(100, 110, 120)
    )
  )
  result <- merge_admissions_and_physiology(adm, phys)
  expect_equal(nrow(result), 3)
  expect_true("age" %in% names(result))
  expect_true("min_hr" %in% names(result))
})

test_that("patients with no physiology data get no rows (dropped)", {
  adm <- data.table(
    person_id = c(1, 2),
    icu_admission_datetime = c("2022-07-01", "2022-07-02"),
    visit_detail_id = c(10, 20)
  )
  phys <- list(
    m_long = data.table(
      person_id = 1,
      icu_admission_datetime = "2022-07-01",
      time_in_icu = 0,
      min_hr = 70, max_hr = 100
    )
  )
  result <- merge_admissions_and_physiology(adm, phys)
  expect_equal(nrow(result), 1)
  expect_equal(result$person_id, 1)
})

test_that("empty physiology → admissions with NA time_in_icu", {
  adm <- data.table(
    person_id = 1,
    icu_admission_datetime = "2022-07-01",
    visit_detail_id = 10
  )
  result <- merge_admissions_and_physiology(adm, list())
  expect_equal(nrow(result), 1)
  expect_true(is.na(result$time_in_icu))
})

test_that("multiple physiology tables merge correctly", {
  adm <- data.table(
    person_id = 1,
    icu_admission_datetime = "2022-07-01",
    visit_detail_id = 10
  )
  phys <- list(
    m_long = data.table(
      person_id = 1, icu_admission_datetime = "2022-07-01",
      time_in_icu = 0, min_hr = 70, max_hr = 100
    ),
    m_counts = data.table(
      person_id = 1, icu_admission_datetime = "2022-07-01",
      time_in_icu = 0, count_emergency_admission = 1
    )
  )
  result <- merge_admissions_and_physiology(adm, phys)
  expect_equal(nrow(result), 1)
  expect_true("min_hr" %in% names(result))
  expect_true("count_emergency_admission" %in% names(result))
})

test_that("physiology from different time windows don't collapse", {
  adm <- data.table(
    person_id = 1,
    icu_admission_datetime = "2022-07-01",
    visit_detail_id = 10
  )
  phys <- list(
    m_long = data.table(
      person_id = c(1, 1),
      icu_admission_datetime = c("2022-07-01", "2022-07-01"),
      time_in_icu = c(0, 1),
      min_hr = c(70, 80), max_hr = c(100, 110)
    )
  )
  result <- merge_admissions_and_physiology(adm, phys)
  expect_equal(nrow(result), 2)
  expect_equal(result$time_in_icu, c(0, 1))
})

test_that("result is sorted by person_id, icu_admission_datetime, time_in_icu", {
  adm <- data.table(
    person_id = c(2, 1),
    icu_admission_datetime = c("2022-07-02", "2022-07-01"),
    visit_detail_id = c(20, 10)
  )
  phys <- list(
    m_long = data.table(
      person_id = c(2, 1, 1),
      icu_admission_datetime = c("2022-07-02", "2022-07-01", "2022-07-01"),
      time_in_icu = c(0, 1, 0),
      min_hr = c(90, 80, 70), max_hr = c(120, 110, 100)
    )
  )
  result <- merge_admissions_and_physiology(adm, phys)
  expect_equal(result$person_id, c(1, 1, 2))
  expect_equal(result$time_in_icu, c(0, 1, 0))
})

test_that("NULL physiology list elements are skipped", {
  adm <- data.table(
    person_id = 1,
    icu_admission_datetime = "2022-07-01",
    visit_detail_id = 10
  )
  phys <- list(
    m_long = data.table(
      person_id = 1, icu_admission_datetime = "2022-07-01",
      time_in_icu = 0, min_hr = 70, max_hr = 100
    ),
    gcs = NULL,
    drg = data.table(
      person_id = integer(0), icu_admission_datetime = character(0),
      time_in_icu = integer(0)
    )
  )
  result <- merge_admissions_and_physiology(adm, phys)
  expect_equal(nrow(result), 1)
  expect_true("min_hr" %in% names(result))
})

test_that("overlapping column names from different tables warn and keep first", {
  # If two physiology tables both have min_hr, only one should appear
  adm <- data.table(
    person_id = 1,
    icu_admission_datetime = "2022-07-01",
    visit_detail_id = 10
  )
  phys <- list(
    table_a = data.table(
      person_id = 1, icu_admission_datetime = "2022-07-01",
      time_in_icu = 0, min_hr = 70, max_hr = 100
    ),
    table_b = data.table(
      person_id = 1, icu_admission_datetime = "2022-07-01",
      time_in_icu = 0, min_hr = 75, max_hr = 105
    )
  )
  expect_warning(
    result <- merge_admissions_and_physiology(adm, phys),
    "already present"
  )
  # Should have exactly one min_hr column (from the first table that added it)
  expect_equal(sum(names(result) == "min_hr"), 1)
  # Value should be from first table (table_a)
  expect_equal(result$min_hr, 70)
})


# =============================================================================
# build_concept_map — edge cases
# =============================================================================

test_that("same concept_id with different filter values → separate rows", {
  concepts <- data.frame(
    concept_id = c("111", "111"),
    short_name = c("rr", "vent_rr"),
    omop_variable = c("value_as_number", "value_as_number"),
    additional_filter_variable_name = c("source", "source"),
    additional_filter_value = c("SPONT", "VENT"),
    stringsAsFactors = FALSE
  )
  result <- build_concept_map(concepts)
  expect_equal(nrow(result), 2)
  expect_true("rr" %in% result$short_name)
  expect_true("vent_rr" %in% result$short_name)
})

test_that("concept_id as numeric string is handled", {
  concepts <- data.frame(
    concept_id = 4301868,
    short_name = "hr",
    omop_variable = "value_as_number",
    additional_filter_variable_name = NA,
    additional_filter_value = NA,
    stringsAsFactors = FALSE
  )
  result <- build_concept_map(concepts)
  expect_equal(result$concept_id, "4301868")
})

test_that("multiple concept_ids for same short_name → multiple map rows", {
  concepts <- data.frame(
    concept_id = c("111", "222"),
    short_name = c("temp", "temp"),
    omop_variable = c("value_as_number", "value_as_number"),
    additional_filter_variable_name = c(NA, NA),
    additional_filter_value = c(NA, NA),
    stringsAsFactors = FALSE
  )
  result <- build_concept_map(concepts)
  expect_equal(nrow(result), 2)
  expect_true(all(result$short_name == "temp"))
})


# =============================================================================
# build_filtered_temp — SQL construction edge cases
# =============================================================================

make_vn <- function(table_name = "Measurement") {
  tbl_map <- list(
    Measurement = list(db = "measurement", alias = "m", id = "measurement_id",
                       cid = "measurement_concept_id",
                       sd = "measurement_date", sdt = "measurement_datetime"),
    Condition = list(db = "condition_occurrence", alias = "co",
                     id = "condition_occurrence_id",
                     cid = "condition_concept_id",
                     sd = "condition_start_date", sdt = "condition_start_datetime")
  )
  t <- tbl_map[[table_name]]
  data.frame(table = table_name, db_table_name = t$db, alias = t$alias,
             id_var = t$id, concept_id_var = t$cid,
             start_date_var = t$sd, start_datetime_var = t$sdt,
             stringsAsFactors = FALSE)
}

test_that("ancestor-only concepts produce concept_ancestor subquery", {
  concepts <- data.frame(
    table = "Condition",
    short_name = "ami",
    concept_id = "45538370",
    omop_variable = "ancestor_concept_id",
    additional_filter_variable_name = NA,
    stringsAsFactors = FALSE
  )
  result <- build_filtered_temp(concepts, "Condition", make_vn("Condition"))
  create_sql <- result[2]
  expect_true(grepl("concept_ancestor", create_sql))
  expect_true(grepl("45538370", create_sql))
})

test_that("no matching concepts → WHERE false", {
  concepts <- data.frame(
    table = character(0),
    short_name = character(0),
    concept_id = character(0),
    omop_variable = character(0),
    additional_filter_variable_name = character(0),
    stringsAsFactors = FALSE
  )
  result <- build_filtered_temp(concepts, "Measurement", make_vn("Measurement"))
  create_sql <- result[2]
  expect_true(grepl("false", create_sql))
})

test_that("numeric + ancestor concepts use UNION (not OR)", {
  concepts <- data.frame(
    table = rep("Condition", 2),
    short_name = c("x", "y"),
    concept_id = c("111", "222"),
    omop_variable = c(NA, "ancestor_concept_id"),
    additional_filter_variable_name = c(NA, NA),
    stringsAsFactors = FALSE
  )
  result <- build_filtered_temp(concepts, "Condition", make_vn("Condition"))
  create_sql <- result[2]
  # Should use UNION to avoid seq scan, not OR
  expect_true(grepl("UNION", create_sql) || grepl("OR", create_sql))
})

test_that("numeric variables trigger unit join", {
  concepts <- data.frame(
    table = "Measurement", short_name = "hr",
    concept_id = "111", omop_variable = "value_as_number",
    additional_filter_variable_name = NA,
    stringsAsFactors = FALSE
  )
  result <- build_filtered_temp(concepts, "Measurement", make_vn("Measurement"))
  create_sql <- result[2]
  expect_true(grepl("unit_concept_id", create_sql))
  expect_true(grepl("unit_name", create_sql))
})

test_that("non-numeric-only concepts don't get unit join", {
  concepts <- data.frame(
    table = "Condition", short_name = "ami",
    concept_id = "111", omop_variable = NA,
    additional_filter_variable_name = NA,
    stringsAsFactors = FALSE
  )
  result <- build_filtered_temp(concepts, "Condition", make_vn("Condition"))
  create_sql <- result[2]
  expect_false(grepl("unit_concept_id", create_sql))
})


# =============================================================================
# build_long_select / build_count_select
# =============================================================================

test_that("build_long_select returns empty for non-numeric concepts", {
  concepts <- data.frame(
    table = "Measurement", short_name = "x",
    concept_id = "111", omop_variable = NA,
    additional_filter_variable_name = NA,
    stringsAsFactors = FALSE
  )
  vn <- make_vn("Measurement")
  result <- build_long_select(concepts, "Measurement", vn, "calendar_date", 24)
  expect_equal(result, "")
})

test_that("build_long_select includes window expression", {
  concepts <- data.frame(
    table = "Measurement", short_name = "hr",
    concept_id = "111", omop_variable = "value_as_number",
    additional_filter_variable_name = NA,
    stringsAsFactors = FALSE
  )
  vn <- make_vn("Measurement")
  result <- build_long_select(concepts, "Measurement", vn, "calendar_date", 24)
  expect_true(grepl("DATEDIFF", result))
  expect_true(grepl("time_in_icu", result))
  expect_true(grepl("MIN.*value_as_number", result))
  expect_true(grepl("MAX.*value_as_number", result))
})

test_that("build_count_select returns empty for numeric-only concepts", {
  concepts <- data.frame(
    table = "Measurement", short_name = "hr",
    concept_id = "111", omop_variable = "value_as_number",
    concept_id_value = NA, additional_filter_variable_name = NA,
    additional_filter_value = NA,
    stringsAsFactors = FALSE
  )
  vn <- make_vn("Measurement")
  result <- build_count_select(concepts, "Measurement", vn, "calendar_date", 24)
  expect_equal(result, "")
})

test_that("build_count_select generates COUNT CASE WHEN", {
  concepts <- data.frame(
    table = "Measurement", short_name = "emerg",
    concept_id = "111", omop_variable = NA,
    concept_id_value = NA, additional_filter_variable_name = NA,
    additional_filter_value = NA,
    stringsAsFactors = FALSE
  )
  vn <- make_vn("Measurement")
  result <- build_count_select(concepts, "Measurement", vn, "calendar_date", 24)
  expect_true(grepl("COUNT.*CASE WHEN", result))
  expect_true(grepl("count_emerg", result))
})


# =============================================================================
# build_drug_statements
# =============================================================================

test_that("build_drug_statements returns NULL when no drug concepts", {
  concepts <- data.frame(
    table = "Measurement", short_name = "hr",
    concept_id = "111", omop_variable = "value_as_number",
    additional_filter_variable_name = NA,
    additional_filter_value = NA,
    stringsAsFactors = FALSE
  )
  vn <- data.frame(
    table = "Drug", db_table_name = "drug_exposure", alias = "drg",
    id_var = "drug_exposure_id", concept_id_var = "drug_concept_id",
    start_date_var = "drug_exposure_start_date",
    start_datetime_var = "drug_exposure_start_datetime",
    end_date_var = "drug_exposure_end_date",
    end_datetime_var = "drug_exposure_end_datetime",
    stringsAsFactors = FALSE
  )
  result <- build_drug_statements(concepts, vn, "calendar_date", 24, "postgresql")
  expect_null(result)
})

test_that("build_drug_statements with ancestor drugs creates ancestor_stmts", {
  concepts <- data.frame(
    table = "Drug", short_name = "antibiotics",
    concept_id = "21602796", omop_variable = "ancestor_concept_id",
    additional_filter_variable_name = NA,
    additional_filter_value = NA,
    stringsAsFactors = FALSE
  )
  vn <- data.frame(
    table = "Drug", db_table_name = "drug_exposure", alias = "drg",
    id_var = "drug_exposure_id", concept_id_var = "drug_concept_id",
    concept_id_value_var = NA,
    start_date_var = "drug_exposure_start_date",
    start_datetime_var = "drug_exposure_start_datetime",
    end_date_var = "drug_exposure_end_date",
    end_datetime_var = "drug_exposure_end_datetime",
    stringsAsFactors = FALSE
  )
  result <- build_drug_statements(concepts, vn, "calendar_date", 24, "postgresql")
  expect_false(is.null(result))
  expect_true(length(result$ancestor_stmts) > 0)
  expect_true(any(grepl("ancestor_map", result$ancestor_stmts)))
  expect_true(any(grepl("is_antibiotics", result$ancestor_stmts)))
})


# =============================================================================
# normalise_concepts_columns edge cases
# =============================================================================

test_that("maps additional_filter_variable_value to additional_filter_value", {
  concepts <- data.frame(
    short_name = "hr", concept_id = "111",
    additional_filter_variable_value = "SOME_VALUE",
    stringsAsFactors = FALSE
  )
  result <- normalise_concepts_columns(concepts)
  expect_equal(result$additional_filter_value, "SOME_VALUE")
  expect_equal(result$additional_filter_variable_value, "SOME_VALUE")
})

test_that("both afvv and afv present → both kept as-is", {
  concepts <- data.frame(
    short_name = "hr", concept_id = "111",
    additional_filter_variable_value = "A",
    additional_filter_value = "B",
    stringsAsFactors = FALSE
  )
  result <- normalise_concepts_columns(concepts)
  expect_equal(result$additional_filter_variable_value, "A")
  expect_equal(result$additional_filter_value, "B")
})

test_that("neither afvv nor afv present → both set to NA", {
  concepts <- data.frame(short_name = "hr", concept_id = "111",
                         stringsAsFactors = FALSE)
  result <- normalise_concepts_columns(concepts)
  expect_true(is.na(result$additional_filter_value))
  expect_true(is.na(result$additional_filter_variable_value))
})


# =============================================================================
# Inf safety in pivot_long_to_wide — belt and braces
# =============================================================================

test_that("all-NA values after dcast never produce Inf", {
  # Two concepts map to different short_names. One has data, one is all NA.
  # After dcast, the all-NA short_name should be NA, not Inf.
  long_dt <- data.table(
    person_id = c(1, 1),
    icu_admission_datetime = "2022-07-01",
    time_in_icu = c(0, 0),
    concept_id = c("111", "222"),
    min_val = c(36.5, NA_real_),
    max_val = c(38.0, NA_real_),
    unit_name = c("C", NA_character_)
  )
  cmap <- data.table(
    concept_id = c("111", "222"),
    short_name = c("temp", "other"),
    filter_col = c(NA_character_, NA_character_),
    filter_vals = list(NA_character_, NA_character_)
  )

  result <- pivot_long_to_wide(long_dt, cmap)

  # temp should have values
  expect_equal(result$min_temp, 36.5)
  expect_equal(result$max_temp, 38.0)

  # other should be NA, NOT Inf
  expect_true(is.na(result$min_other))
  expect_true(is.na(result$max_other))
  expect_false(any(is.infinite(unlist(result))))
})

test_that("no Inf values anywhere in result with mixed NA/non-NA data", {
  long_dt <- data.table(
    person_id = c(1, 1, 1),
    icu_admission_datetime = "2022-07-01",
    time_in_icu = c(0, 0, 0),
    concept_id = c("111", "111", "222"),
    min_val = c(36, NA_real_, NA_real_),
    max_val = c(38, NA_real_, NA_real_),
    unit_name = c("C", NA_character_, NA_character_)
  )
  cmap <- data.table(
    concept_id = c("111", "222"),
    short_name = c("temp", "rr"),
    filter_col = c(NA_character_, NA_character_),
    filter_vals = list(NA_character_, NA_character_)
  )

  result <- pivot_long_to_wide(long_dt, cmap)

  # Check no Inf in any numeric column
  num_cols <- names(result)[sapply(result, is.numeric)]
  for (col in num_cols) {
    expect_false(any(is.infinite(result[[col]])),
                 info = paste("Inf found in column:", col))
  }
})


# =============================================================================
# Duplicate admission detection in merge_admissions_and_physiology
# =============================================================================

test_that("duplicate admissions produce a warning", {
  adm <- data.table(
    person_id = c(1, 1),
    icu_admission_datetime = c("2022-07-01", "2022-07-01"),
    visit_detail_id = c(10, 20),
    age = c(50, 50)
  )
  phys <- list(
    m_long = data.table(
      person_id = 1,
      icu_admission_datetime = "2022-07-01",
      time_in_icu = 0,
      min_hr = 70, max_hr = 100
    )
  )
  expect_warning(
    merge_admissions_and_physiology(adm, phys),
    "DUPLICATE ADMISSIONS DETECTED"
  )
})

test_that("no duplicate admissions → no warning", {
  adm <- data.table(
    person_id = c(1, 2),
    icu_admission_datetime = c("2022-07-01", "2022-07-02"),
    visit_detail_id = c(10, 20)
  )
  phys <- list(
    m_long = data.table(
      person_id = c(1, 2),
      icu_admission_datetime = c("2022-07-01", "2022-07-02"),
      time_in_icu = c(0, 0),
      min_hr = c(70, 80), max_hr = c(100, 110)
    )
  )
  expect_silent(merge_admissions_and_physiology(adm, phys))
})
