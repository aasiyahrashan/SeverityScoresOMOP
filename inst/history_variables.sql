with icu_admission_details as (
	--- We generally want ICU admission information.
	--- But CCHIC doesn't have it, so we use hospital info.
	select
	p.person_id
	, vo.visit_occurrence_id
	, vd.visit_detail_id
	from {schema}.person p
	inner join {schema}.visit_occurrence vo
	on p.person_id = vo.person_id
	-- this should contain ICU stay information, if it exists at all
	left join {schema}.visit_detail vd
	on p.person_id = vd.person_id
	where COALESCE(vd.visit_detail_start_datetime, vo.visit_start_datetime) >= '{start_date}'
	and COALESCE(vd.visit_detail_start_datetime, vo.visit_start_datetime) < '{end_date}'

)

select
adm.*
--- Count queries that describe how many of the comorbidites etc the patient had during that admission. Eg.
--- count (case when observation_concept_id = '4214956'
--- and value_as_concept_id in ('4267414', '4245975') then observation_id end) as comorbid_number
{observation_variables_required}
from icu_admission_details adm
inner join {schema}.observation o
-- making sure the visits match up
-- No date filtering in this query because the concept IDs currently used are history/admission specific.
-- Need to edit the query if this changes for other datasets.
on (adm.visit_detail_id = o.visit_detail_id or adm.visit_detail_id is null)
and adm.visit_occurrence_id = o.visit_occurrence_id
group by adm.person_id, adm.visit_occurrence_id, adm.visit_detail_id
