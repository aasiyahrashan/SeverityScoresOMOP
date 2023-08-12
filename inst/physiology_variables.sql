select
vo.person_id,
vo.visit_occurrence_id,
vo.visit_start_datetime,
DATE_PART('day', m.measurement_datetime - vo.visit_start_datetime) as days_in_icu,
m.measurement_datetime,
m.value_as_number

from {schema}.visit_occurrence vo
inner join {schema}.measurement m

on vo.visit_occurrence_id = m.visit_occurrence_id

where m.measurement_concept_id IN ({concept_ids})
