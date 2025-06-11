--- This calculates the min and max total GCS score if it's stored as individual components with
--- values as concept IDs. I realise I shouldn't hardcode the concept IDs into this file.
--- Will get it to populate using the R code at some point in the future.
--- These are the LOINC concepts with answers. https://athena.ohdsi.org/search-terms/terms/3008223
--- https://athena.ohdsi.org/search-terms/terms/3009094
--- https://athena.ohdsi.org/search-terms/terms/3016335
WITH

@pasted_visits


, measurement_non_aggregated as (
SELECT
 t.*
 ,adm.icu_admission_datetime
,@window_measurement as time_in_icu
--- The admission details table has multiple rows per pasted visit detail.
--- So if visit_detail_id is null in either table, the time join returns multiple identical rows.
--- Making sure I only get one row back at the end, here. The variable I order by does not matter.
, ROW_NUMBER() OVER (
  PARTITION BY t.measurement_id
  ORDER BY t.person_id
) AS rn
 FROM icu_admission_details_multiple_visits adm
 INNER JOIN @schema.measurement t
-- making sure the visits match up, and filtering by number of days in ICU
ON adm.person_id = t.person_id
-- Visit occurrence is not always linked to the other tables.
-- So joining by time instead.
AND (adm.visit_occurrence_id = t.visit_occurrence_id
      OR ((t.visit_occurrence_id IS NULL)
          AND (coalesce(t.measurement_start_datetime, t.measurement_start_date) >= adm.icu_admission_datetime)
          AND (coalesce(t.measurement_start_datetime, t.measurement_start_date)) < adm.icu_discharge_datetime)))
AND @window_measurement >= @first_window
AND @window_measurement <= @last_window
-- Filtering for GCS concepts only
WHERE t.measurement_concept_id IN ('3016335', '3008223', '3009094')
AND value_as_concept_id IS NOT NULL
),

--- Making sure each component of the score is measured at the same time, so we're not
--- matching the eye score from one hour to the verbal score of another.
gcs_concepts as (
	--- selecting non-duplicated values and aggregating
  	SELECT
  	t.person_id
  	-- can't rely on visit occurrence and visit detail IDs being linked to these tables.
    -- So not selecting them. Identifying visits by person ID + admission time
  	,t.icu_admission_datetime
  	,t.time_in_icu
  	,COALESCE(t.measurement_datetime, t.measurement_date) AS measurement_datetime
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
    FROM measurement_non_aggregated as t
    WHERE rn = 1
	---- Getting values that were measured at the same time.
	GROUP BY
	  t.person_id
		,t.icu_admission_datetime
		,@window_measurement
		,COALESCE(t.measurement_datetime, t.measurement_date)
)
	----- Converting the concept IDs to numbers.
gcs_numbers
AS (
	SELECT person_id
		,visit_occurrence_id
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
  ,icu_admission_datetime
	,time_in_icu
	,MIN(gcs_eye + gcs_motor + gcs_verbal) AS min_gcs
	,MAX(gcs_eye + gcs_motor + gcs_verbal) AS max_gcs
FROM gcs_numbers
GROUP BY
  person_id
  ,icu_admission_datetime
	,time_in_icu
