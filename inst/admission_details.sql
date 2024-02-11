--- We generally want ICU admission information.
--- But CCHIC doesn't have it, so we use hospital info.
SELECT p.person_id
	--- Some databases don't have month/day of birth. Others don't have birth datetime.
	--- Imputing DOB as the middle of the year if no further information is available.
	--- Also, not all databases have datetimes, so we have to impute the date as midnight.
	,DATEDIFF(yyyy,
			  COALESCE(p.birth_datetime, DATEFROMPARTS(p.year_of_birth, COALESCE(p.month_of_birth, '06'), COALESCE(p.day_of_birth, '01'))),
			  COALESCE(vd.visit_detail_start_datetime, vd.visit_detail_start_date, vo.visit_start_datetime, vo.visit_start_date)
			  ) as age
	,c_gender.concept_name AS gender
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
