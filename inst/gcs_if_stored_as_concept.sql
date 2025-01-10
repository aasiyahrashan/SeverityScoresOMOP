--- This calculates the min and max total GCS score if it's stored as individual components with
--- values as concept IDs. I realise I shouldn't hardcode the concept IDs into this file.
--- Will get it to populate using the R code at some point in the future.
--- These are the LOINC concepts with answers. https://athena.ohdsi.org/search-terms/terms/3008223
--- https://athena.ohdsi.org/search-terms/terms/3009094
--- https://athena.ohdsi.org/search-terms/terms/3016335
WITH icu_admission_details
AS (
	--- We generally want ICU admission information.
	--- But CCHIC doesn't have it, so we use hospital info.
	SELECT p.person_id
		,vo.visit_occurrence_id
		,vd.visit_detail_id
		,COALESCE(vd.visit_detail_start_datetime, vd.visit_detail_start_date, vo.visit_start_datetime, vo.visit_start_date) AS icu_admission_datetime
	FROM @schema.person p
	INNER JOIN @schema.visit_occurrence vo
		ON p.person_id = vo.person_id
	-- this should contain ICU stay information, if it exists at all
	LEFT JOIN @schema.visit_detail vd
		ON p.person_id = vd.person_id
			AND vo.visit_occurrence_id = vd.visit_occurrence_id
	WHERE COALESCE(vd.visit_detail_start_datetime, vd.visit_detail_start_date,
	                vo.visit_start_datetime, vo.visit_start_date) >= @start_date
		AND COALESCE(vd.visit_detail_start_datetime, vd.visit_detail_start_date,
		              vo.visit_start_datetime, vo.visit_start_date) <= @end_date

	),

	--- Making sure each component of the score is measured at the same time, so we're not
	--- matching the eye score from one hour to the verbal score of another.
gcs_concepts
AS (
	SELECT adm.person_id
		,adm.visit_occurrence_id
		,adm.visit_detail_id
		,@window_measurement AS time_in_icu
		,COALESCE(t.measurement_datetime, t.measurement_date) AS measurement_datetime
		--- The max here is just a method of getting the output into wide format.
		-- There shouldn't be more than one measurement at exactly the same time.
		,MAX(CASE
				WHEN t.measurement_concept_id = '3016335'
					THEN t.value_as_concept_id
				END) gcs_eye
		,MAX(CASE
				WHEN t.measurement_concept_id = '3008223'
					THEN t.value_as_concept_id
				END) gcs_motor
		,MAX(CASE
				WHEN t.measurement_concept_id = '3009094'
					THEN t.value_as_concept_id
				END) gcs_verbal
	FROM icu_admission_details adm
	INNER JOIN @schema.measurement t
		-- making sure the visits match up, and filtering by number of days in ICU
		ON adm.person_id = t.person_id
			AND adm.visit_occurrence_id = t.visit_occurrence_id
			AND (adm.visit_detail_id = t.visit_detail_id OR adm.visit_detail_id IS NULL)
			AND @window_measurement >= '@first_window'
			AND @window_measurement <= '@last_window'
	--- Making sure we get gcs values only. The variables become null otherwise.
	WHERE value_as_concept_id IS NOT NULL
		AND measurement_concept_id IN ('3016335', '3008223', '3009094')
	---- Getting values that were measured at the same time.
	GROUP BY adm.person_id
		,adm.visit_occurrence_id
		,adm.visit_detail_id
		,adm.icu_admission_datetime
		,@window_measurement
		,COALESCE(t.measurement_datetime, t.measurement_date)

	),

	----- Converting the concept IDs to numbers.
gcs_numbers
AS (
	SELECT person_id
		,visit_occurrence_id
		,visit_detail_id
		,time_in_icu
		,CASE
			WHEN gcs_eye = '45877537'
				THEN 1
			WHEN gcs_eye = '45883351'
				THEN 2
			WHEN gcs_eye = '45880465'
				THEN 3
			WHEN gcs_eye = '45880466'
				THEN 4
			END gcs_eye
		,CASE
			WHEN gcs_motor = '45878992'
				THEN 1
			WHEN gcs_motor = '45878993'
				THEN 2
			WHEN gcs_motor = '45879885'
				THEN 3
			WHEN gcs_motor = '45882047'
				THEN 4
			WHEN gcs_motor = '45880467'
				THEN 5
			WHEN gcs_motor = '45880468'
				THEN 6
			END gcs_motor
		,CASE
			WHEN gcs_verbal = '45877384'
				THEN 1
			WHEN gcs_verbal = '45883352'
				THEN 2
			WHEN gcs_verbal = '45877601'
				THEN 3
			WHEN gcs_verbal = '45883906'
				THEN 4
			WHEN gcs_verbal = '45877602'
				THEN 5
			END gcs_verbal
	FROM gcs_concepts
	)
---- Getting min and max per day.
SELECT person_id
	,visit_occurrence_id
	,visit_detail_id
	,time_in_icu
	,MIN(gcs_eye + gcs_motor + gcs_verbal) AS min_gcs
	,MAX(gcs_eye + gcs_motor + gcs_verbal) AS max_gcs
FROM gcs_numbers
GROUP BY person_id
	,visit_occurrence_id
	,visit_detail_id
	,time_in_icu
