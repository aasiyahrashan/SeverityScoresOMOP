with icu_admission_details as (
	--- We generally want ICU admission information.
	--- But CCHIC doesn't have it, so we use hospital info.
	select
	p.person_id
	--- Some databases don't have month/day of birth. Others don't have birth datetime.
	--- Imputing DOB as the middle of the year if no further information is available.
	, DATE_PART('year', COALESCE(vd.visit_detail_start_datetime, vo.visit_start_datetime)) -
	DATE_PART('year', COALESCE(p.birth_datetime,
	make_date(p.year_of_birth, coalesce(p.month_of_birth, '06'), coalesce(p.day_of_birth, '01')))) as age
	, c_gender.concept_name as gender
	, vo.visit_occurrence_id
	, vd.visit_detail_id
	, COALESCE(vd.visit_detail_start_datetime, vo.visit_start_datetime) as icu_admission_datetime
	from {schema}.person p
	inner join {schema}.visit_occurrence vo
	on p.person_id = vo.person_id
	-- this should contain ICU stay information, if it exists at all
	left join {schema}.visit_detail vd
	on p.person_id = vd.person_id
	-- gender concept
	inner join {schema}.concept c_gender
	on p.gender_concept_id = c_gender.concept_id
	where COALESCE(vd.visit_detail_start_datetime, vo.visit_start_datetime) >= '{start_date}'
	and COALESCE(vd.visit_detail_start_datetime, vo.visit_start_datetime) < '{end_date}'

)
select
adm.*
, DATE_PART('day', m.measurement_datetime - adm.icu_admission_datetime) as days_in_icu
--- List of concept IDs to get the min/max of, along with names to assign them to.
-- eg MAX(CASE WHEN m.measurement_concept_id = 4301868 then m.value_as_number END) AS max_hr
{variables_required}
from icu_admission_details adm
inner join {schema}.measurement m
-- making sure the visits match up, and filtering by number of days in ICU
on (adm.visit_detail_id = m.visit_detail_id or adm.visit_detail_id is null)
and adm.visit_occurrence_id = m.visit_occurrence_id
and DATE_PART('day', m.measurement_datetime - adm.icu_admission_datetime) >= {min_day}
and DATE_PART('day', m.measurement_datetime - adm.icu_admission_datetime) < {max_day}
-- getting unit of measure for numeric variables.
inner join {schema}.concept c_unit
on m.unit_concept_id = c_unit.concept_id
-- want min or max values for each visit each day.
GROUP BY adm.person_id, adm.age, adm.gender,
         adm.visit_occurrence_id, adm.visit_detail_id,
		 adm.icu_admission_datetime,
		 DATE_PART('day', m.measurement_datetime - adm.icu_admission_datetime)
