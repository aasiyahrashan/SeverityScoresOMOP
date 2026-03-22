-- Returns one row per ICU stay with admission details and demographics.
-- All physiology data is fetched by separate per-table queries and merged in R.

@pasted_visits

SELECT
  adm.person_id,
  adm.visit_occurrence_id,
  adm.visit_detail_id,
  adm.person_source_value,
  adm.age,
  adm.gender,
  adm.hospital_admission_datetime,
  adm.hospital_discharge_datetime,
  adm.icu_admission_datetime,
  adm.icu_discharge_datetime,
  adm.care_site_id,
  adm.care_site_name,
  adm.death_datetime
FROM icu_admission_details adm
ORDER BY adm.person_id, adm.icu_admission_datetime;
