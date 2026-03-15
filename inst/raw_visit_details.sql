-- This query builds the same CTE structure as paste_disjoint_icu_visits.sql,
-- but uses visit_detail rows as-is without stitching disjoint visits together.
--
  -- Use this when the visit_detail table is already clean and does not need
-- the gap-based merging logic.
--
  -- The output CTEs (icu_admission_details_multiple_visits and icu_admission_details)
-- have the same structure as those in paste_disjoint_icu_visits.sql,
-- so all downstream queries work without modification.

WITH

--- In raw mode, each visit_detail row is its own ICU stay.
--- We still create the multiple_visits CTE for downstream compatibility.
icu_admission_details_multiple_visits AS (
  SELECT
  vd.person_id,
  vd.visit_occurrence_id,
  vd.visit_detail_id,
  vd.visit_detail_id AS new_visit_detail_id,
  COALESCE(vo.visit_start_datetime, vo.visit_start_date) AS hospital_admission_datetime,
  COALESCE(vo.visit_end_datetime, vo.visit_end_date) AS hospital_discharge_datetime,
  COALESCE(vd.visit_detail_start_datetime, vd.visit_detail_start_date) AS icu_admission_datetime,
  COALESCE(vd.visit_detail_end_datetime, vd.visit_detail_end_date) AS icu_discharge_datetime
  FROM @schema.visit_detail vd
  INNER JOIN @schema.visit_occurrence vo
  ON vo.person_id = vd.person_id
  AND (vo.visit_occurrence_id = vd.visit_occurrence_id
       OR (COALESCE(vd.visit_detail_start_datetime, vd.visit_detail_start_date)
           >= COALESCE(vo.visit_start_datetime, vo.visit_start_date)
           AND COALESCE(vd.visit_detail_end_datetime, vd.visit_detail_end_date)
           <= COALESCE(vo.visit_end_datetime, vo.visit_end_date)
           AND vd.visit_occurrence_id IS NULL))
  WHERE vd.visit_detail_concept_id IN (581379, 32037)
  AND COALESCE(vd.visit_detail_start_datetime, vd.visit_detail_start_date) >= @start_date
  AND COALESCE(vd.visit_detail_start_datetime, vd.visit_detail_start_date) <= @end_date
  AND vd.person_id IN (@person_ids)
)

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
