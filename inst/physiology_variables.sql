@pasted_visits

@all_with_queries

@spine_query

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
       ,spine.time_in_icu
       @all_required_variables
       @spine_joins
      -- Admission information needs to be included, even if there are no physiology values
      RIGHT JOIN icu_admission_details adm
      ON spine.person_id = adm.person_id
      AND spine.icu_admission_datetime = adm.icu_admission_datetime
      WHERE spine.time_in_icu IS NOT NULL
      ORDER BY adm.person_id, adm.icu_admission_datetime, spine.time_in_icu;
