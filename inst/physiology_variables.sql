@pasted_visits

WITH
--- De-duplicating, but without the original visit detail IDs
icu_admission_details as
   (SELECT
  	d.person_id
  	,p.person_source_value
  	,@age_query
  	,c.concept_name AS gender
  	,d.visit_occurrence_id
  	,d.new_visit_detail_id AS visit_detail_id
    ,d.hospital_admission_datetime
    ,d.hospital_discharge_datetime
  	,d.icu_admission_datetime
  	,d.icu_discharge_datetime
  	,cs.care_site_id
    ,cs.care_site_name
    ,death.death_datetime
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
  LEFT JOIN @schema.death death ON p.person_id = death.person_id)

@all_with_queries

SELECT adm.person_id
       ,adm.visit_occurrence_id
       --- This is no longer the original visit detail - it's just an ICU stay ID,
       --- since disjoint visits were pasted together.
       ,adm.visit_detail_id
       ,adm.person_source_value
       ,adm.age
       ,adm.gender
       ,adm.hospital_admission_datetime
       ,adm.hospital_discharge_datetime
       ,adm.icu_admission_datetime
       ,adm.icu_discharge_datetime
       ,adm.care_site_id
       ,adm.care_site_name
       ,adm.death_datetime
       ,COALESCE(@all_time_in_icu) AS time_in_icu
       @all_required_variables
       @all_end_join_queries
      -- Admission information needs to be included, even if there are no physiology values
      RIGHT JOIN icu_admission_details adm
      ON COALESCE(@all_person_id) = adm.person_id
      AND COALESCE(@all_icu_admission_datetime) = adm.icu_admission_datetime
      WHERE COALESCE(@all_time_in_icu) IS NOT NULL
      ORDER BY adm.person_id, adm.icu_admission_datetime, COALESCE(@all_time_in_icu);

DROP table icu_admission_details_multiple_visits;
