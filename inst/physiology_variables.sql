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


drug as (
      --- Note, this query will double count overlaps.
      --- If a person has two versions of a single drug, with overalapping start and end dates,
      --- the drug will be double counted.
      SELECT
          t_w.person_id
          ,t_w.visit_occurrence_id
          ,t_w.visit_detail_id
          ,time_in_icu
          @drug_variables
      --- Filtering whole table for string matches so don't need to lateral join the whole thing
      FROM (
          SELECT
          adm.person_id
          ,adm.visit_occurrence_id
          ,adm.visit_detail_id
          ,t.drug_exposure_id
          ,c.concept_name
          ,@window_drug_start as drug_start
          ,@window_drug_end as drug_end
          FROM icu_admission_details adm
          INNER JOIN @schema.drug_exposure t
          ON adm.person_id = t.person_id
          AND adm.visit_occurrence_id = t.visit_occurrence_id
          AND (adm.visit_detail_id = t.visit_detail_id OR adm.visit_detail_id IS NULL)
          INNER JOIN @schema.concept c
          ON c.concept_id = t.drug_concept_id
          WHERE @drug_string_search_expression
      ) t_w
      @drug_join
      GROUP BY
      t_w.person_id
      ,t_w.visit_occurrence_id
      ,t_w.visit_detail_id
      ,time_in_icu
),

SELECT adm.*
           ,COALESCE(m.time_in_icu, o.time_in_icu, co.time_in_icu,
           po.time_in_icu, dev.time_in_icu,
           drg.time_in_icu, vd.time_in_icu) AS time_in_icu
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
LEFT JOIN device dev
        ON adm.person_id           = dev.person_id
       AND adm.visit_occurrence_id = dev.visit_occurrence_id
       AND (adm.visit_detail_id = dev.visit_detail_id OR adm.visit_detail_id IS NULL)
       AND o.time_in_icu = dev.time_in_icu
       AND dev.time_in_icu >= '@first_window'
			 AND dev.time_in_icu < '@last_window'
LEFT JOIN drug drg
        ON adm.person_id           = drg.person_id
       AND adm.visit_occurrence_id = drg.visit_occurrence_id
       AND (adm.visit_detail_id = drg.visit_detail_id OR adm.visit_detail_id IS NULL)
       AND o.time_in_icu = drg.time_in_icu
       AND drg.time_in_icu >= '@first_window'
			 AND drg.time_in_icu < '@last_window'
LEFT JOIN visit_detail_emergency_admission vd
        ON adm.person_id           = vd.person_id
       AND adm.visit_occurrence_id = vd.visit_occurrence_id
       AND (adm.visit_detail_id = vd.visit_detail_id OR adm.visit_detail_id IS NULL)
       AND o.time_in_icu = vd.time_in_icu
       AND vd.time_in_icu >= '@first_window'
			 AND vd.time_in_icu < '@last_window'
WHERE COALESCE(m.time_in_icu, o.time_in_icu, co.time_in_icu, po.time_in_icu,
               dev.time_in_icu, drg.time_in_icu, vd.time_in_icu) IS NOT NULL
