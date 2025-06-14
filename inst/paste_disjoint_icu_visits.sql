-- The queries for most tables are constructed in the R code and added here via the @ parameters.
-- The OMOP visit_detail table in CCHIC sometimes splits what should be continuous visits into multiple rows,
-- even when the end time of one visit is exactly the same as the start time of the next.
-- This query "stitches" those visits back together when:
---  - the visit_detail_concept_id is 581379 or 32037, representing an ICU stay
--   - they belong to the same person,
--   - the gap between the visits is less than 6 hours (to allow time for moves, visits to scanners, etc)
--
-- The output adds the following columns:
--   - new_visit_detail_id: a unique ID for each stitched-together group of contiguous visits
--   - new_start_datetime: the earliest start datetime in the group
--   - new_end_datetime: the latest end datetime in the group
--
-- You can use these new values to consistently group related visit_detail records across clinical data tables.
lagged_visit_details AS (
  SELECT
    vd.*,
    LAG(vd.visit_detail_end_datetime) OVER (
      PARTITION BY vd.person_id, vd.visit_detail_concept_id
      ORDER BY vd.visit_detail_start_datetime
    ) AS prev_end
  FROM @schema.visit_detail vd
  WHERE vd.person_id IN (@person_ids)
),
grouped_visits AS (
  SELECT *,
    SUM(CASE
          WHEN DATEDIFF(MINUTE, prev_end, visit_detail_start_datetime) < 60*6 THEN 0
          ELSE 1
        END) OVER (
          PARTITION BY person_id, visit_occurrence_id, visit_detail_concept_id
          ORDER BY visit_detail_start_datetime
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS group_id
  FROM lagged_visit_details
),
-- renaming time variables here to make future joining easier.
-- Effectively, we have a visit detail table with multiple rows per pasted visit, since
-- we need the original IDs to join to clincial data tables.
pasted_visit_details AS (
  SELECT
    person_id,
    visit_occurrence_id,
    visit_detail_id,
    MIN(visit_detail_start_datetime) OVER (
      PARTITION BY person_id, visit_occurrence_id, group_id
    ) AS visit_detail_start_datetime,
    MAX(visit_detail_end_datetime) OVER (
      PARTITION BY  person_id, visit_occurrence_id, group_id
    ) AS visit_detail_end_datetime,
        MIN(visit_detail_start_date) OVER (
      PARTITION BY person_id, visit_occurrence_id, group_id
    ) AS visit_detail_start_date,
    MAX(visit_detail_end_date) OVER (
      PARTITION BY person_id, visit_occurrence_id, group_id
    ) AS visit_detail_end_date,
    DENSE_RANK() OVER (
      ORDER BY person_id, visit_occurrence_id, group_id
    ) AS new_visit_detail_id
  FROM grouped_visits
),

--- This is called 'multiple_visits', because it joins to the pasted together visit detail table.
--- Therefore, there can be more than one row per pasted visit detail.
icu_admission_details_multiple_visits
AS (
	SELECT vd.person_id
		,vd.visit_occurrence_id
		,vd.visit_detail_id
		,vd.new_visit_detail_id
		,COALESCE(vo.visit_start_datetime, vo.visit_start_date) AS hospital_admission_datetime
    ,COALESCE(vo.visit_end_datetime, vo.visit_end_date) AS hospital_discharge_datetime
		,COALESCE(vd.visit_detail_start_datetime, vd.visit_detail_start_date) AS icu_admission_datetime
		,COALESCE(vd.visit_detail_end_datetime, vd.visit_detail_end_date) AS icu_discharge_datetime
	-- this table has been filtered to include ICU stays only
	FROM pasted_visit_details vd
	-- getting hospital stay information
	INNER JOIN @schema.visit_occurrence vo
  on vo.person_id = vd.person_id
  AND (vo.visit_occurrence_id = vd.visit_occurrence_id
  ---- Some OMOP loads don't include visit_occurrence_ids in the visit_detail table.
       OR
        (COALESCE(vd.visit_detail_start_datetime, vd.visit_detail_start_date) >= COALESCE(vo.visit_start_datetime, vo.visit_start_date)
          AND COALESCE(vd.visit_detail_end_datetime, vd.visit_detail_end_date) <= COALESCE(vo.visit_end_datetime, vo.visit_end_date)
          AND  vd.visit_occurrence_id is null))
	WHERE COALESCE(vd.visit_detail_start_datetime, vd.visit_detail_start_date) >= @start_date
		AND COALESCE(vd.visit_detail_start_datetime, vd.visit_detail_start_date) <= @end_date
	),
	--- De-duplicating, but without the original visit detail IDs
	icu_admission_details as (
		SELECT
		d.person_id
		,p.person_source_value
		,@age_query
		,c.concept_name AS gender
		,d.visit_occurrence_id
		,d.new_visit_detail_id AS visit_detail_id
    ,d.hospital_admission_datetime
    ,d.hospital_discharge_datetime
		,d.icu_admission_datetime
		,d.icu_discharge_datetime
		,cs.care_site_id
	  ,cs.care_site_name
	  ,death.death_datetime
	FROM (
		SELECT DISTINCT
			person_id,
			visit_occurrence_id,
			new_visit_detail_id,
			hospital_admission_datetime,
			hospital_discharge_datetime,
			icu_admission_datetime,
			icu_discharge_datetime
		FROM icu_admission_details_multiple_visits
	) d
	INNER JOIN @schema.person p ON d.person_id = p.person_id
	INNER JOIN @schema.concept c ON p.gender_concept_id = c.concept_id
	LEFT JOIN @schema.care_site cs ON p.care_site_id = cs.care_site_id
	LEFT JOIN @schema.death death ON p.person_id = death.person_id
	)
