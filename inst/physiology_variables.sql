WITH

@pasted_visits

@all_with_queries

SELECT adm.person_id
       ,adm.visit_occurrence_id
       --- This is no longer the original visit detail - it's just an ICU stay ID,
       --- since disjoint visits were pasted together.
       ,adm.visit_detail_id
       ,adm.age
       ,adm.gender
       ,adm.icu_admission_datetime
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
      ORDER BY adm.person_id, adm.icu_admission_datetime, COALESCE(@all_time_in_icu)
