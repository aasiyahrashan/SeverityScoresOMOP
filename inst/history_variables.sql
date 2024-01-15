WITH icu_admission_details
AS (
	  --- We generally want ICU admission information.
	  --- But CCHIC doesn't have it, so we use hospital info.
     SELECT p.person_id
		        ,vo.visit_occurrence_id
		        ,vd.visit_detail_id
		        ,COALESCE(vd.visit_detail_start_datetime, vd.visit_detail_start_date,
		        vo.visit_start_datetime, vo.visit_start_date) AS icu_admission_datetime
	     FROM @schema.person p
 INNER JOIN @schema.visit_occurrence vo
		     ON p.person_id = vo.person_id
	  -- this should contain ICU stay information, if it exists at all
  LEFT JOIN @schema.visit_detail vd
		     ON p.person_id = vd.person_id
			  AND (vo.visit_occurrence_id = vd.visit_occurrence_id OR vd.visit_occurrence_id IS NULL)
	  WHERE COALESCE(vd.visit_detail_start_datetime, vd.visit_detail_start_date, vo.visit_start_datetime, vo.visit_start_date) >= @start_date
		  AND COALESCE(vd.visit_detail_start_datetime, vd.visit_detail_start_date, vo.visit_start_datetime, vo.visit_start_date) < @end_date
	  ),

/*
Count queries that describe how many of the comorbidities the patient had during icu admission.
e.g. COUNT (CASE
                WHEN observation_concept_id = '4214956'
                 AND value_as_concept_id IN ('4267414', '4245975')
                THEN observation_id
            END ) AS comorbid_number
*/
observation_comorbidity
AS (SELECT person_id
           ,visit_occurrence_id
           ,visit_detail_id
           ,COALESCE(observation_datetime, observation_date) AS date_ob
           @observation_variables_required
      FROM @schema.observation
  GROUP BY person_id
           ,visit_occurrence_id
           ,visit_detail_id
           ,COALESCE(observation_datetime, observation_date)
    ),

condition_comorbidity
AS (SELECT person_id
           ,visit_occurrence_id
           ,visit_detail_id
           ,COALESCE(condition_start_datetime, condition_start_date) AS date_ob
           @condition_variables_required
      FROM @schema.condition_occurrence
  GROUP BY person_id
           ,visit_occurrence_id
           ,visit_detail_id
           ,COALESCE(condition_start_datetime, condition_start_date)
    ),

procedure_comorbidity
AS (SELECT person_id
           ,visit_occurrence_id
           ,visit_detail_id
           ,COALESCE(procedure_datetime, procedure_date) AS date_ob
           @procedure_variables_required
      FROM @schema.procedure_occurrence
  GROUP BY person_id
           ,visit_occurrence_id
           ,visit_detail_id
           ,COALESCE(procedure_datetime, procedure_date)
    ),

visit_detail_emergency_admission
AS (SELECT person_id
           ,visit_occurrence_id
           ,visit_detail_id
           ,COALESCE(visit_detail_start_datetime, visit_detail_start_date) AS date_ob
           @visit_detail_variables_required
      FROM @schema.visit_detail
  GROUP BY person_id
           ,visit_occurrence_id
           ,visit_detail_id
           ,COALESCE(visit_detail_start_datetime, visit_detail_start_date)
    )

    SELECT adm.*
           ,DATEDIFF(dd, adm.icu_admission_datetime
           ,COALESCE(o.date_ob, co.date_ob, po.date_ob, vd.date_ob)) AS days_in_icu
           @required_variables
      FROM icu_admission_details adm
-- No date filtering in this query because the concept IDs currently used are history/admission specific.
-- Need to edit the query if this changes for other datasets.
LEFT JOIN observation_comorbidity o
        ON adm.person_id           = o.person_id
       AND adm.visit_occurrence_id = o.visit_occurrence_id
       AND (adm.visit_detail_id = o.visit_detail_id OR adm.visit_detail_id IS NULL)
       AND DATEDIFF(dd, adm.icu_admission_datetime, o.date_ob) >= '@min_day'
			 AND DATEDIFF(dd, adm.icu_admission_datetime, o.date_ob) < '@max_day'
LEFT JOIN condition_comorbidity co
        ON adm.person_id           = co.person_id
       AND adm.visit_occurrence_id = co.visit_occurrence_id
       AND (adm.visit_detail_id = co.visit_detail_id OR adm.visit_detail_id IS NULL)
       AND o.date_ob               = co.date_ob
       AND DATEDIFF(dd, adm.icu_admission_datetime, co.date_ob) >= '@min_day'
			 AND DATEDIFF(dd, adm.icu_admission_datetime, co.date_ob) < '@max_day'
LEFT JOIN procedure_comorbidity po
        ON adm.person_id           = po.person_id
       AND adm.visit_occurrence_id = po.visit_occurrence_id
       AND (adm.visit_detail_id = po.visit_detail_id OR adm.visit_detail_id IS NULL)
       AND o.date_ob               = po.date_ob
       AND DATEDIFF(dd, adm.icu_admission_datetime, po.date_ob) >= '@min_day'
			 AND DATEDIFF(dd, adm.icu_admission_datetime, po.date_ob) < '@max_day'
LEFT JOIN visit_detail_emergency_admission vd
        ON adm.person_id           = vd.person_id
       AND adm.visit_occurrence_id = vd.visit_occurrence_id
       AND (adm.visit_detail_id = vd.visit_detail_id OR adm.visit_detail_id IS NULL)
       AND o.date_ob               = vd.date_ob
       AND DATEDIFF(dd, adm.icu_admission_datetime, vd.date_ob) >= '@min_day'
			 AND DATEDIFF(dd, adm.icu_admission_datetime, vd.date_ob) < '@max_day'
