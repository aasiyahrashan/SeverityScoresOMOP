WITH icu_admission_details
AS (
	  --- We generally want ICU admission information.
	  --- But CCHIC doesn't have it, so we use hospital info.
     SELECT p.person_id
		        ,vo.visit_occurrence_id
		        ,vd.visit_detail_id
	     FROM @schema.person p
 INNER JOIN @schema.visit_occurrence vo
		     ON p.person_id = vo.person_id
	  -- this should contain ICU stay information, if it exists at all
  LEFT JOIN @schema.visit_detail vd
		     ON p.person_id = vd.person_id
			  AND (vo.visit_occurrence_id = vd.visit_occurrence_id OR vd.visit_occurrence_id IS NULL)
	  WHERE COALESCE(vd.visit_detail_start_datetime, vd.visit_detail_start_date, vo.visit_start_datetime, vo.visit_start_date) >= @start_date
		  AND COALESCE(vd.visit_detail_start_datetime, vd.visit_detail_start_date, vo.visit_start_datetime, vo.visit_start_date) < @end_date
	  )
SELECT adm.person_id
       ,adm.visit_occurrence_id
       ,adm.visit_detail_id
       ,count_comorbidity
       ,count_renal_failure
       ,count_emergency_admission
	--- Count queries that describe how many of the comorbidities etc the patient had during that admission. Eg.
	--- count (case when observation_concept_id = '4214956'
	--- and value_as_concept_id in ('4267414', '4245975') then observation_id end) as comorbid_number
FROM icu_admission_details adm
-- making sure the visits match up
-- No date filtering in this query because the concept IDs currently used are history/admission specific.
-- Need to edit the query if this changes for other datasets.
LEFT JOIN ( SELECT person_id
                   ,visit_occurrence_id
                   ,visit_detail_id
                   @observation_variables_required
              FROM @schema.observation
           ) o

	ON adm.person_id = o.person_id
		AND adm.visit_occurrence_id = o.visit_occurrence_id
		AND (adm.visit_detail_id = o.visit_detail_id OR adm.visit_detail_id IS NULL)

-- making sure the visits match up
-- No date filtering in this query because the concept IDs currently used are history/admission specific.
-- Need to edit the query if this changes for other datasets.
LEFT JOIN (SELECT person_id
                  ,visit_occurrence_id
                  ,visit_detail_id
                  @condition_variables_required
             FROM @schema.condition_occurrence
         GROUP BY person_id
                  ,visit_occurrence_id
                  ,visit_detail_id
           ) AS co
	ON adm.person_id = co.person_id
		AND adm.visit_occurrence_id = co.visit_occurrence_id
		AND (adm.visit_detail_id = co.visit_detail_id OR adm.visit_detail_id IS NULL)
LEFT JOIN (SELECT person_id
                  ,visit_occurrence_id
                  ,visit_detail_id
                  @visit_detail_variables_required
             FROM @schema.visit_detail
         GROUP BY person_id
                  ,visit_occurrence_id
                  ,visit_detail_id
                ) AS vd
          	ON adm.person_id = vd.person_id
		AND adm.visit_occurrence_id = vd.visit_occurrence_id
		AND (adm.visit_detail_id = vd.visit_detail_id)
GROUP BY adm.person_id
	     ,adm.visit_occurrence_id
	     ,adm.visit_detail_id
       ,count_comorbidity
       ,count_renal_failure
       ,count_emergency_admission
