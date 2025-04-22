-- The OMOP visit_detail table sometimes splits what should be continuous visits into multiple rows,
-- even when the end time of one visit is exactly the same as the start time of the next.
-- This query "stitches" those visits back together when:
--   - they belong to the same person,
--   - and the same Caboodle department (represented by visit_detail_source_value),
--   - and there is no gap between the visits (i.e., the previous visit ends exactly when the next one starts).
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
      PARTITION BY person_id, visit_detail_source_value
      ORDER BY visit_detail_start_datetime
    ) AS prev_end
  FROM hic_cc_004.visit_detail
),
grouped_visits AS (
  SELECT *,
    SUM(CASE
          WHEN visit_detail_start_datetime = prev_end THEN 0
          ELSE 1
        END) OVER (
          PARTITION BY person_id, visit_detail_source_value
          ORDER BY visit_detail_start_datetime
          ROWS UNBOUNDED PRECEDING
        ) AS group_id
  FROM lagged_visit_details
),
relabled_visit_details AS (
  SELECT
    *,
    MIN(visit_detail_start_datetime) OVER (
      PARTITION BY person_id, visit_detail_source_value, group_id
    ) AS new_visit_detail_start_datetime,
    MAX(visit_detail_end_datetime) OVER (
      PARTITION BY person_id, visit_detail_source_value, group_id
    ) AS new_visit_detail_end_datetime,
    DENSE_RANK() OVER (
      ORDER BY person_id, visit_detail_source_value, group_id
    ) AS new_visit_detail_id
  FROM grouped_visits
)
select * from relabled_visit_details
order by
person_id,
visit_detail_start_datetime
