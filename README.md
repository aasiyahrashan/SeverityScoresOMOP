# SeverityScoresOMOP

R package that extracts physiology data and computes APACHE II and SOFA severity scores from OMOP CDM databases. Also extracts and summarises other clinical variables into configurable time windows.

## Features

- APACHE II and SOFA scores from OMOP CDM v5.4 databases.
- Flexible variable extraction with customisable time windows.
- Supports PostgreSQL and SQL Server.
- Three ICU stay identification modes: **paste** (default, stitches disjoint visit_detail rows), **raw** (uses visit_detail as-is), and **ground truth** (uses external admission times).

## Installation

```r
devtools::install_github("aasiyahrashan/SeverityScoresOMOP")
```

## Quick Start

### 1. Prepare the Concept Mapping File

Copy `inst/example_concepts.csv`, update OMOP concept IDs for your database, and save it under version control. Do not change `short_name` values for severity score variables. See [Concept Mapping CSV Format](#concept-mapping-csv-format) below.

### 2. Connect and Extract

```r
library(SeverityScoresOMOP)

omop_conn <- omop_connect("PostgreSQL", "localhost", "dbname", 5432, "user", "password")

data <- get_score_variables(
  omop_conn,
  dialect = "postgresql",
  schema = "your_schema",
  start_date = "2022-07-01",
  end_date = "2022-07-31",
  first_window = 0,
  last_window = 6,
  concepts_file_path = "path/to/concepts.csv",
  severity_score = c("APACHE II", "SOFA")
)
```

Each time window is a separate row. `first_window`/`last_window` define the range (inclusive), `cadence` (hours, default 24) defines window size.

For **raw mode**, add `visit_mode = "raw"`. For **ground truth mode**, see `?ground_truth_config`.

### 3. Compute Scores

```r
# APACHE II
data <- fix_units(data)
data <- fix_implausible_values(data)
data <- calculate_apache_ii_score(data)

# SOFA
data <- calculate_sofa_score(data)
```

## Visit Modes

| `visit_mode` | Description |
|---|---|
| `"paste"` | (Default) Stitches disjoint `visit_detail` rows. `paste_gap_hours` (default 6) controls max gap. |
| `"raw"` | Uses `visit_detail` as-is. For clean databases. |
| `"ground_truth"` | Uses external ICU times. Requires a `gt_config` object (see `?ground_truth_config`) and a joining schema mapping external IDs to OMOP `person_id`/`visit_occurrence_id`. |

## Concept Mapping CSV Format

The CSV defines how OMOP concepts map to extracted variables. Add to version control.

### Required Columns

| Column | Description |
|---|---|
| score | `"APACHE II"`, `"SOFA"`, or `"APACHE II, SOFA"`. Filter for extraction. |
| short_name | Output column name stem. Do not change for core severity score variables. |
| table | OMOP table: `Measurement`, `Observation`, `Condition`, `Procedure`, `Device`, `Drug`, or `Visit Detail`. |
| concept_id | OMOP concept ID, or search string (for `concept_name`/`concept_code` searches). Multiple rows for multiple values. |
| omop_variable | How data is read. See below. |
| concept_id_value | For `value_as_concept_id` filtering. |
| additional_filter_variable_name | Optional extra filter column (e.g. `measurement_source_value`). One per `short_name`. |
| additional_filter_variable_value | Values for the above filter. Multiple rows for multiple values. |

### `omop_variable` Options

| Value | Description | Output |
|---|---|---|
| `value_as_number` | Min/max/unit of the numeric value field. | `min_`, `max_`, `unit_` |
| `value_as_concept_id` | Count where `value_as_concept_id` matches `concept_id_value`. | `count_` |
| `concept_name` | Count via `LIKE '%term%'` on concept name (case insensitive). **Slow — requires full concept table scan (>60s). Use numeric `concept_id` instead where possible.** | `count_` |
| `concept_code` | Count via `LIKE '%term%'` on concept code. **Broad matching:** `I21` also matches `I210`, `I219`, `AI21`, etc. Verify results or use numeric `concept_id` instead. | `count_` |
| `ancestor_concept_id` | Count descendants of the specified ancestor via `concept_ancestor`. | `count_` |
| *(blank)* | Count occurrences of the `concept_id`. | `count_` |

## OMOP Database Requirements

- OMOP CDM v5.4 or compatible.
- Required tables: `person`, `visit_occurrence`, `visit_detail` (not required in ground truth mode).
- ICU stays must use `visit_detail_concept_id` 581379 or 32037.
- `visit_occurrence_id` in `visit_detail` should be filled in; if NULL, the code falls back to datetime overlap matching.
- Queries benefit from composite indexes such as `measurement(person_id, measurement_concept_id)`. Ask your DBA if queries are slow.

## Ground Truth Mode

Use when you have external ICU admission/discharge times (e.g. from a clinical registry). Requires:
- A ground truth dataframe with patient ID, encounter ID, ICU admission/discharge datetimes.
- A joining schema in your OMOP database mapping external IDs to `person_id` and `visit_occurrence_id`.

```r
gt <- ground_truth_config(
  df = gt_data,
  joining_schema = "your_joining_schema",
  joining_person_table = "person_map",
  joining_visit_table = "visit_map",
  gt_person_key = "PatientKey",
  omop_person_key = "PatientKey",
  gt_encounter_key = "EncounterKey",
  omop_encounter_key = "EncounterKey",
  gt_icu_admission_col = "IcuAdmissionDatetime",
  gt_icu_discharge_col = "IcuDischargeDatetime"
)

data <- get_score_variables(..., visit_mode = "ground_truth", ground_truth = gt)
```

See `?ground_truth_config` for full parameter list including optional hospital admission/discharge columns.

## TODO

- Add support for OMOP 5.3.1.
- Improve handling of PaO2, PaCO2, FiO2, and MAP (min/max/ratios).
- Add mortality prediction calculation.
- Refactor unit conversion and implausible value checks to use lookup tables exclusively.
- Include vasopressors in SOFA calculation.
- Handle multiple units of measure per person/variable/time.

## Questions or Issues?

[Open an issue](https://github.com/aasiyahrashan/SeverityScoresOMOP/issues) on GitHub.
