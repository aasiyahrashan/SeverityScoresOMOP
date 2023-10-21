--- This calculates the min and max total GCS score if it's stored as individual components with
--- values as concept IDs. I realise I shouldn't hardcode the concept IDs into this file.
--- Will get it to populate using the R code at some point in the future.
--- These are the LOINC concepts with answers. https://athena.ohdsi.org/search-terms/terms/3008223
--- https://athena.ohdsi.org/search-terms/terms/3009094
--- https://athena.ohdsi.org/search-terms/terms/3016335
with icu_admission_details as (
	--- We generally want ICU admission information.
	--- But CCHIC doesn't have it, so we use hospital info.
	select
	p.person_id
	, vo.visit_occurrence_id
	, vd.visit_detail_id
	, COALESCE(vd.visit_detail_start_datetime, vd.visit_detail_start_date,
	           vo.visit_start_datetime, vo.visit_start_date) as icu_admission_datetime
	from {schema}.person p
	inner join {schema}.visit_occurrence vo
	on p.person_id = vo.person_id
	-- this should contain ICU stay information, if it exists at all
	left join {schema}.visit_detail vd
	on p.person_id = vd.person_id
	and (vo.visit_occurrence_id = vd.visit_occurrence_id or vd.visit_occurrence_id is null)
	where COALESCE(vd.visit_detail_start_datetime, vd.visit_detail_start_date,
	               vo.visit_start_datetime, vo.visit_start_date) >= '{start_date}'
	and COALESCE(vd.visit_detail_start_datetime, vd.visit_detail_start_date,
	             vo.visit_start_datetime, vo.visit_start_date) < '{end_date}'

),
--- Making sure each component of the score is measured at the same time, so we're not
--- matching the eye score from one hour to the verbal score of another.
gcs_concepts as (
select
adm.person_id,
adm.visit_occurrence_id,
adm.visit_detail_id,
DATE_PART('day', coalesce(m.measurement_datetime, m.measurement_date) - adm.icu_admission_datetime) as days_in_icu,
coalesce(m.measurement_datetime, m.measurement_date) as measurement_datetime
--- The max here is just a method of getting the output into wide format.
-- There shouldn't be more than one measurement at exactly the same time.
, MAX(CASE WHEN m.measurement_concept_id = '3016335' then m.value_as_concept_id END) gcs_eye
, MAX(CASE WHEN m.measurement_concept_id = '3008223' then m.value_as_concept_id END) gcs_motor
, MAX(CASE WHEN m.measurement_concept_id = '3009094' then m.value_as_concept_id END) gcs_verbal
from icu_admission_details adm
left join {schema}.measurement m
-- making sure the visits match up, and filtering by number of days in ICU
on adm.person_id = m.person_id
and adm.visit_occurrence_id = m.visit_occurrence_id
and (adm.visit_detail_id = m.visit_detail_id or adm.visit_detail_id is null)
and DATE_PART('day', coalesce(m.measurement_datetime, m.measurement_date) - adm.icu_admission_datetime) >= {min_day}
and DATE_PART('day', coalesce(m.measurement_datetime, m.measurement_date) - adm.icu_admission_datetime) < {max_day}
--- Making sure we get gcs values only. The variables become null otherwise.
where value_as_concept_id is not null
and measurement_concept_id IN ('3016335', '3008223', '3009094')
---- Getting values that were measured at the same time.
group by
	adm.person_id, adm.visit_occurrence_id, adm.visit_detail_id,
	adm.icu_admission_datetime,
	DATE_PART('day', coalesce(m.measurement_datetime, m.measurement_date) - adm.icu_admission_datetime),
	coalesce(m.measurement_datetime, m.measurement_date)
	),
----- Converting the concept IDs to numbers.
gcs_numbers as (
select
person_id,
visit_occurrence_id,
visit_detail_id,
days_in_icu,
case when gcs_eye = '45877537' then 1
	 when gcs_eye = '45883351' then 2
	 when gcs_eye = '45880465' then 3
	 when gcs_eye = '45880466' then 4
	 end gcs_eye,
case when gcs_motor = '45878992' then 1
	 when gcs_motor = '45878993' then 2
	 when gcs_motor = '45879885' then 3
	 when gcs_motor = '45882047' then 4
	 when gcs_motor = '45880467' then 5
	 when gcs_motor = '45880468' then 6
	 end gcs_motor,
case when gcs_verbal = '45877384' then 1
	 when gcs_verbal = '45883352' then 2
	 when gcs_verbal = '45877601' then 3
	 when gcs_verbal = '45883906' then 4
	 when gcs_verbal = '45877602' then 5
	 end gcs_verbal
from gcs_concepts
)
---- Getting min and max per day.
select
person_id,
visit_occurrence_id,
visit_detail_id,
days_in_icu,
MIN(gcs_eye + gcs_motor + gcs_verbal) as min_gcs,
MAX(gcs_eye + gcs_motor + gcs_verbal) as max_gcs
from gcs_numbers
group by person_id, visit_occurrence_id, visit_detail_id, days_in_icu
