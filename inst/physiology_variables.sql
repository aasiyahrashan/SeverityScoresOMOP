select
vo.person_id
, vo.visit_occurrence_id
, vo.visit_start_datetime
, DATE_PART('day', m.measurement_datetime - vo.visit_start_datetime) as days_in_icu
--- List of concept IDs to get the min/max of, along with names to assign them to.
-- eg MAX(CASE WHEN m.measurement_concept_id = 4301868 then m.value_as_number END) AS max_hr
{variables_required}
from {schema}.visit_occurrence vo
inner join {schema}.measurement m
on vo.visit_occurrence_id = m.visit_occurrence_id
-- filter by date, and by the number of days after ICU admission
and vo.visit_start_datetime >= '{start_date}' and vo.visit_start_datetime < '{end_date}'
and DATE_PART('day', m.measurement_datetime - vo.visit_start_datetime) >= {min_day}
and DATE_PART('day', m.measurement_datetime - vo.visit_start_datetime) < {max_day}
-- want min or max values for each visit each day.
GROUP BY vo.visit_occurrence_id, DATE_PART('day', m.measurement_datetime - vo.visit_start_datetime)

