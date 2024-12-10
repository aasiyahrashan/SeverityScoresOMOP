-- The queries for most tables are constructed in the R code and added here via the @ parameters.
WITH icu_admission_details
AS (
	--- We generally want ICU admission information.
	--- But CCHIC doesn't have it, so we use hospital info.
	SELECT p.person_id
		--- Some databases don't have month/day of birth. Others don't have birth datetime.
		--- Imputing DOB as the middle of the year if no further information is available.
		--- Also, not all databases have datetimes, so we have to impute the date as midnight.
		,@age_query
		,c_gender.concept_name AS gender
		,vo.visit_occurrence_id
		,vd.visit_detail_id
		,COALESCE(vd.visit_detail_start_datetime, vd.visit_detail_start_date,
		vo.visit_start_datetime, vo.visit_start_date) AS icu_admission_datetime
	FROM @schema.person p
	INNER JOIN @schema.visit_occurrence vo
	ON p.person_id = vo.person_id
	-- this should contain ICU stay information, if it exists at all
	LEFT JOIN @schema.visit_detail vd
	ON vo.visit_occurrence_id = vd.visit_occurrence_id
	-- gender concept
	INNER JOIN @schema.concept c_gender
	ON p.gender_concept_id = c_gender.concept_id
	WHERE COALESCE(vd.visit_detail_start_datetime, vd.visit_detail_start_date,
	                vo.visit_start_datetime, vo.visit_start_date) >= @start_date
		AND COALESCE(vd.visit_detail_start_datetime, vd.visit_detail_start_date,
		              vo.visit_start_datetime, vo.visit_start_date) <= @end_date
	)

@all_with_queries

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
)

SELECT adm.*
           ,COALESCE(@all_time_in_icu) AS time_in_icu
           @all_required_variables
      FROM icu_admission_details adm
      @all_end_join_queries
      WHERE COALESCE(@all_time_in_icu) IS NOT NULL
