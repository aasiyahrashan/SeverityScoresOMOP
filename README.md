# SeverityScoresOMOP

**SeverityScoresOMOP** is an R package that extracts physiology data and computes the ICU APACHE II and SOFA severity scores from OMOP Common Data Model (CDM) databases. It also provides tools for summarizing other clinical variables into time windows for flexible analysis.

---

## Features

- Calculates APACHE II and SOFA severity scores from OMOP CDM v5.4 databases.
- Extracts and summarizes physiological and clinical variables into customizable time windows.
- Supports SQL Server and PostgreSQL (most testing on PostgreSQL).

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

```r
data <- get_score_variables(
  omop_conn,
  sql_dialect = "your_sql_dialect",
  schema = "your_schema_name",
  start_date = "2022-07-01",
  end_date = "2022-07-31",
  first_window = 0, # Negative values can get data from before the ICU stay, as long as the data is within the hospital visit.
  last_window = 6, # Gets the first 7 days of data, since `cadence` is set to 24
  concepts_file_path = "path_to_your_concepts_file",
  severity_score = c("APACHE II", "SOFA"),
  age_method = "dob", # or "year_only"
  cadence = 24, # Each window spans 24 hours
  window_start_point = "calendar_date" # or "icu_admission_time"
)
dbDisconnect(omop_conn)
```
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
- **Required tables:** `Person`, `Visit Occurrence`, and `Visit Detail`
    - `Visit Occurrence`: Represents hospital stays.
    - `Visit Detail`: Represents ICU/ward stays or bed/room moves.
- ICU stays must have `visit_detail_concept_id` set to `581379` ("Inpatient Critical Care Facility").
- Each `Visit Detail` row must have `visit_occurrence_id` filled in.
- ICU stays in the same hospital visit and separated by less than 6 hours are merged into a single stay (to avoid splitting on brief moves, e.g., for scans).

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
