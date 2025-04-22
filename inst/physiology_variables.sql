WITH
-- The queries for most tables are constructed in the R code and added here via the @ parameters.
-- The OMOP visit_detail table sometimes splits what should be continuous visits into multiple rows,
-- even when the end time of one visit is exactly the same as the start time of the next.
-- This query "stitches" those visits back together when:
---  - the visit_detail_concept_id is 581379, representing an ICU stay
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
    *,
    LAG(visit_detail_end_datetime) OVER (
      PARTITION BY person_id, visit_detail_concept_id
      ORDER BY visit_detail_start_datetime
    ) AS prev_end
  FROM @schema.visit_detail
  WHERE visit_detail_concept_id = 581379
),
grouped_visits AS (
  SELECT *,
    SUM(CASE
          WHEN DATEDIFF(MINUTE, prev_end, visit_detail_start_datetime) < 60*6 THEN 0
          ELSE 1
        END) OVER (
          PARTITION BY person_id, visit_detail_concept_id
          ORDER BY visit_detail_start_datetime
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS group_id
  FROM lagged_visit_details
),
pasted_visit_details AS (
  SELECT
    *,
    MIN(visit_detail_start_datetime) OVER (
      PARTITION BY person_id, visit_detail_concept_id, group_id
    ) AS new_visit_detail_start_datetime,
    MAX(visit_detail_end_datetime) OVER (
      PARTITION BY person_id, visit_detail_concept_id, group_id
    ) AS new_visit_detail_end_datetime,
        MIN(visit_detail_start_date) OVER (
      PARTITION BY person_id, visit_detail_concept_id, group_id
    ) AS new_visit_detail_start_date,
    MAX(visit_detail_end_date) OVER (
      PARTITION BY person_id, visit_detail_concept_id, group_id
    ) AS new_visit_detail_end_date,
    DENSE_RANK() OVER (
      ORDER BY person_id, visit_detail_concept_id, group_id
    ) AS new_visit_detail_id
  FROM grouped_visits
),
-- renaming time variables here to make future joining easier.
-- Effectively, we have a visit detail table with multiple rows per pasted visit, since
-- we need the original IDs to join to clincial data tables.
renamed_visit_details AS (
  SELECT
  person_id,
  visit_occurrence_id,
  visit_detail_id,
  new_visit_detail_id,
  new_visit_detail_start_datetime visit_detail_start_datetime,
  new_visit_detail_end_datetime visit_detail_end_datetime,
  new_visit_detail_start_date visit_detail_start_date,
  new_visit_detail_end_date, visit_detail_end_date
  FROM pasted_visit_details
),
--- This is called 'multiple_visits', because it joins to the pasted together visit detail table.
--- Therefore, there can be more than one row per pasted visit detail.
icu_admission_details_multiple_visits
AS (
	--- We generally want ICU admission information, but will use hospital admisison info if necessary.
	SELECT p.person_id
		--- Some databases don't have month/day of birth. Others don't have birth datetime.
		--- Imputing DOB as the middle of the year if no further information is available.
		--- Also, not all databases have datetimes, so we have to impute the date as midnight.
		,@age_query
		,c_gender.concept_name AS gender
		,vo.visit_occurrence_id
		,vd.visit_detail_id
		,vd.new_visit_detail_id
		,COALESCE(vd.visit_detail_start_datetime, vd.visit_detail_start_date,
		vo.visit_start_datetime, vo.visit_start_date) AS icu_admission_datetime
		,COALESCE(vd.visit_detail_end_datetime, vd.visit_detail_end_date,
		vo.visit_end_datetime, vo.visit_end_date) AS icu_discharge_datetime
	FROM @schema.person p
	INNER JOIN @schema.visit_occurrence vo
	ON p.person_id = vo.person_id
	-- this should contain ICU stay information, if it exists at all
	LEFT JOIN renamed_visit_details vd
	ON vo.visit_occurrence_id = vd.visit_occurrence_id
	-- gender concept
	INNER JOIN @schema.concept c_gender
	ON p.gender_concept_id = c_gender.concept_id
	WHERE COALESCE(vd.visit_detail_start_datetime, vd.visit_detail_start_date,
	                vo.visit_start_datetime, vo.visit_start_date) >= @start_date
		AND COALESCE(vd.visit_detail_start_datetime, vd.visit_detail_start_date,
		              vo.visit_start_datetime, vo.visit_start_date) <= @end_date
	),
	--- De-duplicating, but without the original visit detail IDs
	icu_admission_details as (
	select distinct
	person_id
	,age
	,gender
	,visit_occurrence_id
	--- don't like this, because it's confusing. But leaving for now.
	,new_visit_detail_id visit_detail_id
	,icu_admission_datetime
	,icu_discharge_datetime
	from icu_admission_details_multiple_visits
	)

@all_with_queries

SELECT adm.person_id
       ,adm.visit_occurrence_id
       ,adm.visit_detail_id
       ,adm.age
       ,adm.gender
       ,adm.icu_admission_datetime
       ,COALESCE(@all_time_in_icu) AS time_in_icu
       @all_required_variables
      @all_end_join_queries
      -- Admission information needs to be included, even if there are no physiology values
      RIGHT JOIN icu_admission_details adm
      ON COALESCE(@all_person_id) = adm.person_id
      AND COALESCE(@all_icu_admission_datetime) = adm.icu_admission_datetime
      WHERE COALESCE(@all_time_in_icu) IS NOT NULL
      ORDER BY adm.person_id, adm.icu_admission_datetime, COALESCE(@all_time_in_icu)
