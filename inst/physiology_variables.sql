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
	)
SELECT adm.*
	, @window_measurement as days_in_icu
	--- List of concept IDs to get the min/max of, along with names to assign them to.
	-- eg MAX(CASE WHEN m.measurement_concept_id = 4301868 then m.value_as_number END) AS max_hr
	@variables_required
FROM icu_admission_details adm
INNER JOIN @schema.measurement m
	-- making sure the visits match up, and filtering by number of days in ICU
	ON adm.person_id = m.person_id
	AND adm.visit_occurrence_id = m.visit_occurrence_id
	AND (adm.visit_detail_id = m.visit_detail_id OR adm.visit_detail_id IS NULL)
	AND @window_measurement >= '@min_day'
	AND @window_measurement < '@max_day'
-- getting unit of measure for numeric variables.
LEFT JOIN @schema.concept c_unit ON m.unit_concept_id = c_unit.concept_id
	AND m.unit_concept_id IS NOT NULL
-- want min or max values for each visit each day.
GROUP BY adm.person_id
	,adm.visit_occurrence_id
	,adm.visit_detail_id
	,adm.icu_admission_datetime
	,@window_measurement
