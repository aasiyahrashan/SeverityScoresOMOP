WITH icu_admission_details
AS (
	--- We generally want ICU admission information.
	--- But CCHIC doesn't have it, so we use hospital info.
	SELECT p.person_id
		--- Some databases don't have month/day of birth. Others don't have birth datetime.
		--- Imputing DOB as the middle of the year if no further information is available.
		--- Also, not all databases have datetimes, so we have to impute the date as midnight.
		,vo.visit_occurrence_id
		,vd.visit_detail_id
		,COALESCE(vd.visit_detail_start_datetime, vd.visit_detail_start_date, vo.visit_start_datetime, vo.visit_start_date) AS icu_admission_datetime
	FROM @schema.person p
	INNER JOIN @schema.visit_occurrence vo ON p.person_id = vo.person_id
	-- this should contain ICU stay information, if it exists at all
	LEFT JOIN @schema.visit_detail vd ON vo.visit_occurrence_id = vd.visit_occurrence_id
	-- gender concept
	INNER JOIN @schema.concept c_gender ON p.gender_concept_id = c_gender.concept_id
	WHERE COALESCE(vd.visit_detail_start_datetime, vd.visit_detail_start_date, vo.visit_start_datetime, vo.visit_start_date) >= @start_date
		AND COALESCE(vd.visit_detail_start_datetime, vd.visit_detail_start_date, vo.visit_start_datetime, vo.visit_start_date) <= @end_date
	),

--- Measurement
measurement as (
  	SELECT t.person_id
           ,t.visit_occurrence_id
           ,t.visit_detail_id
  	, @window_measurement as time_in_icu
  	--- List of concept IDs to get the min/max of, along with names to assign them to.
  	-- eg MAX(CASE WHEN m.measurement_concept_id = 4301868 then m.value_as_number END) AS max_hr
  	@measurement_variables
  FROM icu_admission_details adm
  INNER JOIN @schema.measurement t
  	-- making sure the visits match up, and filtering by number of days in ICU
  	ON adm.person_id = t.person_id
  	AND adm.visit_occurrence_id = t.visit_occurrence_id
  	AND (adm.visit_detail_id = t.visit_detail_id OR adm.visit_detail_id IS NULL)
  	AND @window_measurement >= '@first_window'
  	AND @window_measurement < '@last_window'
  -- getting unit of measure for numeric variables.
  LEFT JOIN @schema.concept c_unit ON t.unit_concept_id = c_unit.concept_id
  	AND t.unit_concept_id IS NOT NULL
  -- want min or max values for each visit each day.
  GROUP BY t.person_id
  	,t.visit_occurrence_id
  	,t.visit_detail_id
  	,@window_measurement
	),

--- The remaining queries mainly use counts
/*
e.g. COUNT (CASE
                WHEN observation_concept_id = '4214956'
                 AND value_as_concept_id IN ('4267414', '4245975')
                THEN observation_id
            END ) AS comorbid_number
*/
--- Observation
observation AS (SELECT t.person_id
           ,t.visit_occurrence_id
           ,t.visit_detail_id
           ,@window_observation AS time_in_icu
           @observation_variables
      FROM icu_admission_details adm
      INNER JOIN @schema.observation t
      ON adm.person_id = t.person_id
      AND adm.visit_occurrence_id = t.visit_occurrence_id
      AND (adm.visit_detail_id = t.visit_detail_id OR adm.visit_detail_id IS NULL)
  GROUP BY t.person_id
           ,t.visit_occurrence_id
           ,t.visit_detail_id
           ,@window_observation
    ),

condition AS (SELECT t.person_id
           ,t.visit_occurrence_id
           ,t.visit_detail_id
           ,@window_condition AS time_in_icu
           @condition_variables
      FROM icu_admission_details adm
      INNER JOIN @schema.condition_occurrence t
      ON adm.person_id = t.person_id
      AND adm.visit_occurrence_id = t.visit_occurrence_id
      AND (adm.visit_detail_id = t.visit_detail_id OR adm.visit_detail_id IS NULL)
  GROUP BY t.person_id
           ,t.visit_occurrence_id
           ,t.visit_detail_id
           ,@window_condition
    ),

procedure AS (SELECT t.person_id
           ,t.visit_occurrence_id
           ,t.visit_detail_id
           ,@window_procedure AS time_in_icu
           @procedure_variables
      FROM icu_admission_details adm
      INNER JOIN @schema.procedure_occurrence t
      ON adm.person_id = t.person_id
      AND adm.visit_occurrence_id = t.visit_occurrence_id
      AND (adm.visit_detail_id = t.visit_detail_id OR adm.visit_detail_id IS NULL)
  GROUP BY t.person_id
           ,t.visit_occurrence_id
           ,t.visit_detail_id
           ,@window_procedure
    ),

device AS (SELECT t.person_id
           ,t.visit_occurrence_id
           ,t.visit_detail_id
           ,@window_device AS time_in_icu
           @device_variables
      FROM icu_admission_details adm
      INNER JOIN @schema.device_exposure t
      ON adm.person_id = t.person_id
      AND adm.visit_occurrence_id = t.visit_occurrence_id
      AND (adm.visit_detail_id = t.visit_detail_id OR adm.visit_detail_id IS NULL)
  GROUP BY t.person_id
           ,t.visit_occurrence_id
           ,t.visit_detail_id
           ,@window_device
    ),

drug as (
      --- Note, this query will double count overlaps.
      --- If a person has two versions of a single drug, with overalapping start and end dates,
      --- the drug will be double counted.
      SELECT
          t.person_id
          ,t.visit_occurrence_id
          ,t.visit_detail_id
          ,time_in_icu
          @drug_variables
      --- Filtering whole table for string matches so don't need to lateral join the whole thing
      FROM (
          SELECT *
          FROM icu_admission_details adm
          INNER JOIN @schema.drug_exposure t
          INNER JOIN @schema.concept c
          ON adm.person_id = t.person_id
          AND adm.visit_occurrence_id = t.visit_occurrence_id
          AND (adm.visit_detail_id = t.visit_detail_id OR adm.visit_detail_id IS NULL)
          ON c.concept_id = de.drug_concept_id
          WHERE lower(c.concept_name) similar to lower(@drug_string_search_expression)
      ) t
      LEFT JOIN LATERAL
      --- For each row in the table, creating
      generate_series(
          @window_drug_start
          @window_drug_end
      --- The 'on true' condition just means that every row in the drug table gets joined to
      --- the corresponding time_in_icu rows created by generate_series.
      ) AS time_in_icu on TRUE
      GROUP BY t.person_id, time_in_icu;
),

visit_detail_emergency_admission AS (SELECT t.person_id
           ,t.visit_occurrence_id
           ,t.visit_detail_id
           --- has to be 0 since ICU admission datetime is derived from the same variables.
           ,0 AS time_in_icu
           @visit_detail_variables
      FROM icu_admission_details adm
      INNER JOIN @schema.visit_detail t
      ON adm.person_id = t.person_id
      AND adm.visit_occurrence_id = t.visit_occurrence_id
      AND (adm.visit_detail_id = t.visit_detail_id OR adm.visit_detail_id IS NULL)
  GROUP BY t.person_id
           ,t.visit_occurrence_id
           ,t.visit_detail_id
    )

SELECT adm.*
           ,COALESCE(m.time_in_icu, o.time_in_icu, co.time_in_icu,
           po.time_in_icu, vd.time_in_icu) AS time_in_icu
           @all_required_variables
      FROM icu_admission_details adm
LEFT JOIN measurement m
        ON adm.person_id           = m.person_id
       AND adm.visit_occurrence_id = m.visit_occurrence_id
       AND (adm.visit_detail_id = m.visit_detail_id OR adm.visit_detail_id IS NULL)
       AND m.time_in_icu >= '@first_window'
			 AND m.time_in_icu < '@last_window'
LEFT JOIN observation o
        ON adm.person_id           = o.person_id
       AND adm.visit_occurrence_id = o.visit_occurrence_id
       AND (adm.visit_detail_id = o.visit_detail_id OR adm.visit_detail_id IS NULL)
       AND m.time_in_icu = o.time_in_icu
       AND o.time_in_icu >= '@first_window'
			 AND o.time_in_icu < '@last_window'
LEFT JOIN condition co
        ON adm.person_id           = co.person_id
       AND adm.visit_occurrence_id = co.visit_occurrence_id
       AND (adm.visit_detail_id = co.visit_detail_id OR adm.visit_detail_id IS NULL)
       AND o.time_in_icu = co.time_in_icu
       AND co.time_in_icu >= '@first_window'
			 AND co.time_in_icu < '@last_window'
LEFT JOIN procedure po
        ON adm.person_id           = po.person_id
       AND adm.visit_occurrence_id = po.visit_occurrence_id
       AND (adm.visit_detail_id = po.visit_detail_id OR adm.visit_detail_id IS NULL)
       AND o.time_in_icu = po.time_in_icu
       AND po.time_in_icu >= '@first_window'
			 AND po.time_in_icu < '@last_window'
LEFT JOIN device de
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
WHERE COALESCE(m.time_in_icu, o.time_in_icu, co.time_in_icu, po.time_in_icu,
               de.time_in_icu, vd.time_in_icu) IS NOT NULL
