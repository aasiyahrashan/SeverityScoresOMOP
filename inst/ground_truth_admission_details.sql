-- This query builds the same CTE structure as paste_disjoint_icu_visits.sql,
-- but using pre-resolved ground truth data passed as a VALUES clause.
--
-- The ground truth IDs are resolved to OMOP person_id and visit_occurrence_id
-- in R before this query runs. The VALUES clause (@ground_truth_values) contains
-- rows of (person_id, visit_occurrence_id, icu_admission_datetime, icu_discharge_datetime).
--
-- Hospital admission/discharge times are always taken from visit_occurrence,
-- unless optional expressions are provided via @gt_hospital_admission_expr
-- and @gt_hospital_discharge_expr.
--
-- The output CTEs (icu_admission_details_multiple_visits and icu_admission_details)
-- have the same structure as those in paste_disjoint_icu_visits.sql,
-- so all downstream queries (physiology_variables.sql, gcs_if_stored_as_concept.sql)
-- work without modification.

WITH

ground_truth_data (person_id, visit_occurrence_id, icu_admission_datetime, icu_discharge_datetime) AS (
  VALUES @ground_truth_values
),

--- Build the icu_admission_details_multiple_visits CTE.
--- In ground truth mode, there is exactly one row per ICU stay (no multiple visit_detail rows),
--- but we keep the same CTE name and structure for compatibility with downstream queries.
--- visit_detail_id is set to ROW_NUMBER since there is no original visit_detail_id.
icu_admission_details_multiple_visits AS (
  SELECT
    gt.person_id,
    gt.visit_occurrence_id,
    ROW_NUMBER() OVER (ORDER BY gt.person_id, gt.icu_admission_datetime) AS visit_detail_id,
    ROW_NUMBER() OVER (ORDER BY gt.person_id, gt.icu_admission_datetime) AS new_visit_detail_id,
    COALESCE(@gt_hospital_admission_expr,
             COALESCE(vo.visit_start_datetime, vo.visit_start_date)) AS hospital_admission_datetime,
    COALESCE(@gt_hospital_discharge_expr,
             COALESCE(vo.visit_end_datetime, vo.visit_end_date)) AS hospital_discharge_datetime,
    gt.icu_admission_datetime,
    gt.icu_discharge_datetime
  FROM ground_truth_data gt
  INNER JOIN @schema.visit_occurrence vo
    ON gt.visit_occurrence_id = vo.visit_occurrence_id
  WHERE gt.icu_admission_datetime >= @start_date
    AND gt.icu_admission_datetime <= @end_date
)

--- De-duplicate and add demographics (same structure as paste_disjoint_icu_visits.sql).
, icu_admission_details AS (
  SELECT
    d.person_id,
    p.person_source_value,
    @age_query,
    c.concept_name AS gender,
    d.visit_occurrence_id,
    d.new_visit_detail_id AS visit_detail_id,
    d.hospital_admission_datetime,
    d.hospital_discharge_datetime,
    d.icu_admission_datetime,
    d.icu_discharge_datetime,
    cs.care_site_id,
    cs.care_site_name,
    death.death_datetime
  FROM (
    SELECT DISTINCT
      person_id,
      visit_occurrence_id,
      new_visit_detail_id,
      hospital_admission_datetime,
      hospital_discharge_datetime,
      icu_admission_datetime,
      icu_discharge_datetime
    FROM icu_admission_details_multiple_visits
  ) d
  INNER JOIN @schema.person p ON d.person_id = p.person_id
  INNER JOIN @schema.concept c ON p.gender_concept_id = c.concept_id
  LEFT JOIN @schema.care_site cs ON p.care_site_id = cs.care_site_id
  LEFT JOIN @schema.death death ON p.person_id = death.person_id
)
