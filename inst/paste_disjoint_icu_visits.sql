-- The OMOP visit_detail table sometimes splits what should be continuous visits into multiple rows,
-- even when the end time of one visit is exactly the same as the start time of the next.
-- This query "stitches" those visits back together when:
---  - the visit_detail_concept_id is 581379, representing an ICU stay
--   - they belong to the same person,
--   - the gap between the visits is less than 1 hour (to allow time for moves, visits to scanners, etc)
--
-- The output adds the following columns:
--   - new_visit_detail_id: a unique ID for each stitched-together group of contiguous visits
--   - new_start_datetime: the earliest start datetime in the group
--   - new_end_datetime: the latest end datetime in the group
--
-- You can use these new values to consistently group related visit_detail records across clinical data tables.
WITH lagged_visit_details AS (
  SELECT
    *,
    LAG(visit_detail_end_datetime) OVER (
      PARTITION BY person_id, visit_detail_concept_id
      ORDER BY visit_detail_start_datetime
    ) AS prev_end
  FROM @schema.visit_detail
	where visit_detail_concept_id = '581379'
),
grouped_visits AS (
  SELECT *,
    SUM(CASE
		WHEN visit_detail_start_datetime - prev_end < INTERVAL '1 hour' THEN 0
          ELSE 1
        END) OVER (
          PARTITION BY person_id, visit_detail_concept_id
          ORDER BY visit_detail_start_datetime
          ROWS UNBOUNDED PRECEDING
        ) AS group_id
  FROM lagged_visit_details
),
relabled_visit_details AS (
  SELECT
    *,
    MIN(visit_detail_start_datetime) OVER (
      PARTITION BY person_id, visit_detail_concept_id, group_id
    ) AS new_visit_detail_start_datetime,
    MAX(visit_detail_end_datetime) OVER (
      PARTITION BY person_id, visit_detail_concept_id, group_id
    ) AS new_visit_detail_end_datetime,
    DENSE_RANK() OVER (
      ORDER BY person_id, visit_detail_concept_id, group_id
    ) AS new_visit_detail_id
  FROM grouped_visits
)

select
person_id,
new_visit_detail_id,
new_visit_detail_start_datetime,
new_visit_detail_end_datetime,
visit_detail_start_datetime,
visit_detail_end_datetime,
visit_detail_source_value
FROM relabled_visit_details
order by person_id, new_visit_detail_start_datetime
