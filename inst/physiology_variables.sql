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

SELECT adm.*
           ,COALESCE(@all_time_in_icu) AS time_in_icu
           @all_required_variables
      @all_end_join_queries
      -- Admission information needs to be included, even if there are no physiology values
      RIGHT JOIN icu_admission_details adm
      ON COALESCE(@all_person_id) = adm.person_id
      AND COALESCE(@all_visit_occurrence_id) = adm.visit_occurrence_id
      AND (COALESCE(@all_visit_detail_id) = adm.visit_detail_id
            OR adm.visit_detail_id IS NULL)
      WHERE COALESCE(@all_time_in_icu) IS NOT NULL
