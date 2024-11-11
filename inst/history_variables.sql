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
			  AND vo.visit_occurrence_id = vd.visit_occurrence_id
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
AS (SELECT o.person_id
           ,o.visit_occurrence_id
           ,o.visit_detail_id
           ,@window_observation AS time_in_icu
           @observation_variables_required
      FROM icu_admission_details adm
      INNER JOIN @schema.observation o
      ON adm.person_id = o.person_id
      AND adm.visit_occurrence_id = o.visit_occurrence_id
      AND (adm.visit_detail_id = o.visit_detail_id OR adm.visit_detail_id IS NULL)
  GROUP BY o.person_id
           ,o.visit_occurrence_id
           ,o.visit_detail_id
           ,@window_observation
    ),

condition_comorbidity
AS (SELECT co.person_id
           ,co.visit_occurrence_id
           ,co.visit_detail_id
           ,@window_condition AS time_in_icu
           @condition_variables_required
      FROM icu_admission_details adm
      INNER JOIN @schema.condition_occurrence co
      ON adm.person_id = co.person_id
      AND adm.visit_occurrence_id = co.visit_occurrence_id
      AND (adm.visit_detail_id = co.visit_detail_id OR adm.visit_detail_id IS NULL)
  GROUP BY co.person_id
           ,co.visit_occurrence_id
           ,co.visit_detail_id
           ,@window_condition
    ),

procedure_comorbidity
AS (SELECT po.person_id
           ,po.visit_occurrence_id
           ,po.visit_detail_id
           ,@window_procedure AS time_in_icu
           @procedure_variables_required
      FROM icu_admission_details adm
      INNER JOIN @schema.procedure_occurrence po
      ON adm.person_id = po.person_id
      AND adm.visit_occurrence_id = po.visit_occurrence_id
      AND (adm.visit_detail_id = po.visit_detail_id OR adm.visit_detail_id IS NULL)
  GROUP BY po.person_id
           ,po.visit_occurrence_id
           ,po.visit_detail_id
           ,@window_procedure
    ),

device_comorbidity
AS (SELECT de.person_id
           ,de.visit_occurrence_id
           ,de.visit_detail_id
           ,@window_device AS time_in_icu
           @device_variables_required
      FROM icu_admission_details adm
      INNER JOIN @schema.device_exposure de
      ON adm.person_id = de.person_id
      AND adm.visit_occurrence_id = de.visit_occurrence_id
      AND (adm.visit_detail_id = de.visit_detail_id OR adm.visit_detail_id IS NULL)
  GROUP BY de.person_id
           ,de.visit_occurrence_id
           ,de.visit_detail_id
           ,@window_device
    ),

visit_detail_emergency_admission
AS (SELECT vd.person_id
           ,vd.visit_occurrence_id
           ,vd.visit_detail_id
           --- has to be 0 since ICU admission datetime is derived from the same variables.
           ,0 AS time_in_icu
           @visit_detail_variables_required
      FROM icu_admission_details adm
      INNER JOIN @schema.visit_detail vd
      ON adm.person_id = vd.person_id
      AND adm.visit_occurrence_id = vd.visit_occurrence_id
      AND (adm.visit_detail_id = vd.visit_detail_id OR adm.visit_detail_id IS NULL)
  GROUP BY vd.person_id
           ,vd.visit_occurrence_id
           ,vd.visit_detail_id
    )

    SELECT adm.*
           ,COALESCE(o.time_in_icu, co.time_in_icu, po.time_in_icu, vd.time_in_icu) AS time_in_icu
           @required_variables
      FROM icu_admission_details adm
LEFT JOIN observation_comorbidity o
        ON adm.person_id           = o.person_id
       AND adm.visit_occurrence_id = o.visit_occurrence_id
       AND (adm.visit_detail_id = o.visit_detail_id OR adm.visit_detail_id IS NULL)
       AND o.time_in_icu >= '@first_window'
			 AND o.time_in_icu < '@last_window'
LEFT JOIN condition_comorbidity co
        ON adm.person_id           = co.person_id
       AND adm.visit_occurrence_id = co.visit_occurrence_id
       AND (adm.visit_detail_id = co.visit_detail_id OR adm.visit_detail_id IS NULL)
       AND o.time_in_icu = co.time_in_icu
       AND co.time_in_icu >= '@first_window'
			 AND co.time_in_icu < '@last_window'
LEFT JOIN procedure_comorbidity po
        ON adm.person_id           = po.person_id
       AND adm.visit_occurrence_id = po.visit_occurrence_id
       AND (adm.visit_detail_id = po.visit_detail_id OR adm.visit_detail_id IS NULL)
       AND o.time_in_icu = po.time_in_icu
       AND po.time_in_icu >= '@first_window'
			 AND po.time_in_icu < '@last_window'
LEFT JOIN device_comorbidity de
        ON adm.person_id           = de.person_id
       AND adm.visit_occurrence_id = de.visit_occurrence_id
       AND (adm.visit_detail_id = de.visit_detail_id OR adm.visit_detail_id IS NULL)
       AND o.time_in_icu = de.time_in_icu
       AND de.time_in_icu >= '@first_window'
			 AND de.time_in_icu < '@last_window'
LEFT JOIN visit_detail_emergency_admission vd
        ON adm.person_id           = vd.person_id
       AND adm.visit_occurrence_id = vd.visit_occurrence_id
       AND (adm.visit_detail_id = vd.visit_detail_id OR adm.visit_detail_id IS NULL)
       AND o.time_in_icu = vd.time_in_icu
       AND vd.time_in_icu >= '@first_window'
			 AND vd.time_in_icu < '@last_window'
WHERE COALESCE(o.time_in_icu, co.time_in_icu, po.time_in_icu, de.time_in_icu,
                vd.time_in_icu) IS NOT NULL
