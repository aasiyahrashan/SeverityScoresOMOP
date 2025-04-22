paste_visit_details_query <- function(table_name, variable_names, prev_alias){
  paste_visit_details_query <-
    glue("
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
         ")

}

