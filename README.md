# SeverityScoresOMOP

**SeverityScoresOMOP** is an R package that extracts physiology data and computes the ICU APACHE II and SOFA severity scores from OMOP Common Data Model (CDM) databases. It also provides tools for summarizing other clinical variables into time windows for flexible analysis.

---

## Features

- Calculates APACHE II and SOFA severity scores from OMOP CDM v5.4 databases.
- Extracts and summarizes physiological and clinical variables into customizable time windows.
- Supports SQL Server and PostgreSQL (most testing on PostgreSQL).
- Three modes for identifying ICU stays:
  - **Paste mode** (default): Stitches disjoint `visit_detail` rows together if the gap is below a configurable threshold.
  - **Raw mode**: Uses `visit_detail` rows as-is, for databases where visit_detail is already clean.
  - **Ground truth mode**: Uses an external ground truth table (e.g. from a CSV) to define ICU stays, joined to OMOP via a configurable mapping schema.

---

## Installation

Install via [`devtools`](https://github.com/r-lib/devtools):

```r
devtools::install_github("aasiyahrashan/SeverityScoresOMOP")
```

---

## Quick Start

### 1. Prepare the Concept Mapping File

- Copy `inst/example_concepts.csv` from the package.
- Rename it, e.g., `your_dataset_name_concepts.csv`.
- Update OMOP concept IDs as needed but **do not change the `short_name` values** for severity score variables.
- Place the file somewhere accessible to your project and add it to version control.

### 2. Connect to Your OMOP Database

```r
library(SeverityScoresOMOP)

# Example: Connect to a PostgreSQL OMOP database
omop_conn <- omop_connect(
  driver = "PostgreSQL",
  host = "localhost",
  dbname = "dbname",
  port = 5432,
  user = "user",
  password = "password"
)
```

### 3. Extract Variables and Calculate Scores

#### Option A: Using OMOP visit_detail (paste mode — default)

```r
data <- get_score_variables(
  omop_conn,
  dialect = "your_sql_dialect",
  schema = "your_schema_name",
  start_date = "2022-07-01",
  end_date = "2022-07-31",
  first_window = 0,
  last_window = 6,
  concepts_file_path = "path_to_your_concepts_file",
  severity_score = c("APACHE II", "SOFA"),
  age_method = "dob",
  cadence = 24,
  window_start_point = "calendar_date",
  visit_mode = "paste",        # Default. Stitches disjoint visit_detail rows.
  paste_gap_hours = 6           # Default. Maximum gap (hours) before visits are treated as separate.
)
dbDisconnect(omop_conn)
```

#### Option B: Using OMOP visit_detail as-is (raw mode)

Use this when the `visit_detail` table is already clean and does not need stitching.

```r
data <- get_score_variables(
  omop_conn,
  dialect = "your_sql_dialect",
  schema = "your_schema_name",
  start_date = "2022-07-01",
  end_date = "2022-07-31",
  first_window = 0,
  last_window = 6,
  concepts_file_path = "path_to_your_concepts_file",
  severity_score = c("APACHE II", "SOFA"),
  visit_mode = "raw"
)
```

#### Option C: Using a ground truth table

Use this when you have an external source of truth for ICU admission/discharge times
(e.g. a clinical registry) and want to use those times instead of deriving them from
OMOP `visit_detail`.

**Prerequisites:**
- A ground truth CSV/dataframe with at least: a patient identifier, an encounter identifier,
  ICU admission datetime, and ICU discharge datetime.
- A "joining schema" in your OMOP database that maps the ground truth identifiers to
  OMOP `person_id` and `visit_occurrence_id`.

```r
# Load ground truth
gt_data <- read.csv("path/to/ground_truth.csv")

# Create config object
gt <- ground_truth_config(
  df = gt_data,
  joining_schema = "your_joining_schema",
  joining_person_table = "person_mapping_table",
  joining_visit_table = "visit_mapping_table",
  gt_person_key = "PatientDurableKey",
  omop_person_key = "PatientDurableKey",
  gt_encounter_key = "EncounterKey",
  omop_encounter_key = "EncounterKey",
  gt_icu_admission_col = "CrossIcuStayStartInstant",
  gt_icu_discharge_col = "CrossIcuStayEndInstant"
  # gt_hospital_admission_col and gt_hospital_discharge_col are optional.
  # If omitted, hospital times are taken from OMOP visit_occurrence.
)

data <- get_score_variables(
  omop_conn,
  dialect = "your_sql_dialect",
  schema = "your_schema_name",
  start_date = "2022-07-01",
  end_date = "2022-07-31",
  first_window = 0,
  last_window = 6,
  concepts_file_path = "path_to_your_concepts_file",
  severity_score = c("APACHE II", "SOFA"),
  cadence = 24,
  window_start_point = "icu_admission_time",
  visit_mode = "ground_truth",
  ground_truth = gt
)
```

**What happens during ground truth mode:**
1. The ground truth dataframe is validated (required columns, no NAs in keys, admission before discharge).
2. A bidirectional check runs (disable with `run_validation = FALSE`):
   - Warns about ground truth patients not found in OMOP.
   - Warns about OMOP ICU patients not found in ground truth.
3. Ground truth IDs are resolved to OMOP `person_id` and `visit_occurrence_id`
   via the joining schema.
4. Physiology variables are extracted from OMOP clinical tables using the ground truth
   ICU admission times for windowing.
5. Original ground truth columns (e.g. IcuStayRegistryKey) are joined back onto
   the result for debugging and downstream joins.

**Joining back daily ground truth data:**
If your ground truth table also has per-person-day variables (e.g. ventilation status),
you can join them back to the output after extraction:

```r
# The output contains person_id, icu_admission_datetime, and time_in_icu.
# Join your daily ground truth data using these keys.
data <- left_join(data, daily_ground_truth,
                  by = c("person_id", "time_in_icu"))
```

**Storing the ground truth extraction query:**
The SQL query used to extract the ground truth CSV from its source database should be
stored in your analysis repository (not this package) for reproducibility.

- **Note:** Each time window is a separate row in the output. `first_window` and `last_window` define the window range (inclusive), `cadence` (in hours) defines window size.

### 4. Standardize Data and Compute Scores

```r
# APACHE II
data <- fix_apache_ii_units(data)
data <- fix_implausible_values_apache_ii(data)
data <- calculate_apache_ii_score(data, imputation = "none")   # Complete cases only
print(data$apache_ii_score_no_imputation)
data <- calculate_apache_ii_score(data, imputation = "normal") # With normal imputation
print(data$apache_ii_score)

# SOFA
data <- fix_sofa_units(data)
data <- fix_implausible_values_sofa(data)
data <- calculate_sofa_score(data, imputation = "none")
data <- calculate_sofa_score(data)
print(data$sofa_score)

# Explore variable availability and distributions
availability_df <- get_physiology_variable_availability(data)
View(availability_df)
get_physiology_variable_distributions(data)
```

---

## Visit Mode Reference

| Parameter | Value | Description |
|-----------|-------|-------------|
| `visit_mode` | `"paste"` | (Default) Stitches disjoint `visit_detail` rows. Use `paste_gap_hours` to control the maximum gap. |
| `visit_mode` | `"raw"` | Uses `visit_detail` as-is. For clean databases that don't need stitching. |
| `visit_mode` | `"ground_truth"` | Uses an external ground truth table. Requires `ground_truth_df` and joining schema parameters. |
| `paste_gap_hours` | Numeric (default 6) | Only for `"paste"` mode. Maximum gap in hours before visits are treated as separate stays. |

---

## Concept Mapping CSV Format

The mapping CSV defines how OMOP concepts are linked to extracted variables. **Add this file to version control.**

### Required Columns

| Column Name                       | Description                                                                                          |
|------------------------------------|------------------------------------------------------------------------------------------------------|
| score                             | "APACHE II" or "SOFA", or "APACHE II, SOFA". Used as a filter for severity score extraction. (I intend to remove this, since I primarily use the package to extract non-severity score variables)                              |
| short_name                        | Used in output column names; must not be changed for core severity score variables.                 |
| table                             | OMOP table name: `Measurement`, `Observation`, `Condition`, `Procedure`, `Device`, `Drug`, or `Visit detail` (for `emergency_admission`). |
| concept_id                        | OMOP concept ID(s) or string (for concept name search; add multiple rows for multiple concepts).    |
| omop_variable                     | **See table below. Specifies how data is read from the OMOP field.**                                |
| concept_id_value                  | OMOP concept ID for filtering (used with `value_as_concept_id`). Multiple values = multiple rows.   |
| name_of_value                     | Human-readable label for `concept_id_value` (for documentation).                                    |
| additional_filter_variable_name   | (Optional) Extra filter variable (e.g., by source name). Only one per `short_name`.                 |
| additional_filter_variable_value  | (Optional) Filter values for the above. Multiple rows for multiple values.                          |
| Note                              | (Optional) For documentation only; not used in code.                                                |

### `omop_variable` Options

This column controls how each variable is extracted and represented in the output. Choose the option that matches your use case. To match multiple `concept_id`s, search strings, or `values_as_concept_id`s, create multiple rows in the table, using the same short_name:

| Value                     | Description                                                                                                             | Output Columns Generated                                   |
|---------------------------|-------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------|
| value_as_number           | Summarises the `value_as_number` field in OMOP into minimum, maximum and unit. Note that the unit field assumes that units of measure are consistent per patient and variable. (I intend to fix this at some point.)                                               | `min_short_name`, `max_short_name`, `unit_short_name`      |
| value_as_concept_id       | Counts occurrences where `value_as_concept_id` matches `concept_id_value`.                                              | `count_short_name`                                         |
| concept_name              | Counts occurrences where `concept_name` matches the search string, as per the SQL LIKE `%search_string%` operator. Case insensitive.                                                      | `count_short_name`                                         |
| concept_code              | Counts occurrences where `concept_code` matches the search string, Counts occurrences where `concept_name` matches the search string, as per the SQL LIKE `%search_string%` operator. Case insensitive.                                                      | `count_short_name`                                         |
| ancestor_concept_id       | Counts occurrences where the concept ID in the table matches any of the decendents of the value specified in the `concept_id` field of this table. Uses the OMOP `concept_ancestor` table.                                             | `count_short_name`                                         |
| (blank)                   | If left blank, counts matching occurrences of the `concept_id` in the specified table.                                        | `count_short_name`                                         |

---

## OMOP Database Requirements

- Must be OMOP CDM version 5.4 or compatible (planned support for 5.3.1).
- **Required tables:** `Person`, `Visit Occurrence`, and `Visit Detail` (not required in ground truth mode)
    - `Visit Occurrence`: Represents hospital stays.
    - `Visit Detail`: Represents ICU/ward stays or bed/room moves.
- ICU stays must have `visit_detail_concept_id` set to `581379` ("Inpatient Critical Care Facility") or `32037` ("Intensive Care"). Not applicable in ground truth mode.
- Each `Visit Detail` row should ideally have `visit_occurrence_id` filled in. If it is NULL, the code falls back to matching by datetime overlap with `visit_occurrence` (not required in ground truth mode if the joining schema provides it).
- ICU stays in the same hospital visit and separated by less than `paste_gap_hours` (default 6) hours are merged into a single stay in paste mode.

### Ground Truth Mode Requirements

In addition to the standard OMOP tables (person, visit_occurrence, measurement, etc.):
- A **joining schema** in the OMOP database with:
  - A table mapping external patient identifiers to `person_id`.
  - A table mapping external encounter identifiers to `visit_occurrence_id`.
- A **ground truth dataframe** (loaded from CSV or similar) with columns for:
  - Patient identifier (matching the joining schema)
  - Encounter identifier (matching the joining schema)
  - ICU admission datetime
  - ICU discharge datetime
  - Optionally: hospital admission/discharge datetimes

---

## Dependencies

- [OHDSI SqlRender](https://github.com/OHDSI/SqlRender) (for SQL translation)
    - Note: SQL dialect support is limited; tested on SQL Server and PostgreSQL.

---

## TODO

- Improve documentation.
- Add support for OMOP 5.3.1.
- Improve handling of PaO2, PaCO2, FiO2, and MAP (min/max/ratios).
- Add mortality prediction calculation.
- Refactor unit conversion and implausible value checks to avoid code duplication.
- Include vasopressors in SOFA calculation.
- Handle multiple units of measure per person/variable/time.

---

## Questions or Issues?

If you need help or want to report a bug, please [open an issue](https://github.com/aasiyahrashan/SeverityScoresOMOP/issues) on GitHub.

---
