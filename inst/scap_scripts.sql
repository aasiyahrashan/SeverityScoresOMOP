---- Random scripts to access ICU stay information for both OMOP and Caboodle. Saving here for convenience

---- Get all pasted ICU visits
WITH
-- The queries for most tables are constructed in the R code and added here via the @ parameters.
-- The OMOP visit_detail table in CCHIC sometimes splits what should be continuous visits into multiple rows,
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
  FROM hic_cc_004.visit_detail
  WHERE visit_detail_concept_id = 581379
),
grouped_visits AS (
  SELECT *,
    SUM(CASE
          WHEN (EXTRACT(EPOCH FROM (visit_detail_start_datetime - prev_end)) / 60) < 60*6 THEN 0
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
	SELECT p.person_id
		--- Some databases don't have month/day of birth. Others don't have birth datetime.
		--- Imputing DOB as the middle of the year if no further information is available.
		--- Also, not all databases have datetimes, so we have to impute the date as midnight.
		--- DO AGE QUERY ELSEWHERE, SINCE IT"S POSSIBLE FOR IT TO CHANGE OVER VISITS"
		, (EXTRACT(YEAR FROM CAST(COALESCE(vd.visit_detail_start_datetime, vd.visit_detail_start_date,
        vo.visit_start_datetime, vo.visit_start_date) AS DATE)) - EXTRACT(YEAR FROM CAST(COALESCE(p.birth_datetime, TO_DATE(TO_CHAR(p.year_of_birth,'0000')||'-'||TO_CHAR(COALESCE(p.month_of_birth, '06'),'00')||'-'||TO_CHAR(COALESCE(p.day_of_birth, '01'),'00'), 'YYYY-MM-DD')) AS DATE))) as age
		,c_gender.concept_name AS gender
		,vo.visit_occurrence_id
		,vd.visit_detail_id
		,vd.new_visit_detail_id
		,COALESCE(vd.visit_detail_start_datetime, vd.visit_detail_start_date) AS icu_admission_datetime
		,COALESCE(vd.visit_detail_end_datetime, vd.visit_detail_end_date) AS icu_discharge_datetime
	FROM hic_cc_004.person p
	INNER JOIN hic_cc_004.visit_occurrence vo
	ON p.person_id = vo.person_id
	-- this table has been filtered to include ICU stays only
	INNER JOIN renamed_visit_details vd
	ON vo.visit_occurrence_id = vd.visit_occurrence_id
	-- gender concept
	INNER JOIN hic_cc_004.concept c_gender
	ON p.gender_concept_id = c_gender.concept_id
	WHERE COALESCE(vd.visit_detail_start_datetime, vd.visit_detail_start_date) >= '2021-12-01'
		AND COALESCE(vd.visit_detail_start_datetime, vd.visit_detail_start_date) <= '2021-12-31'
	),
	--- De-duplicating, but without the original visit detail IDs
	icu_admission_details as (
		SELECT
		d.person_id,
		@age_query
		c.concept_name AS gender,
		d.visit_occurrence_id,
		d.new_visit_detail_id AS visit_detail_id,
		d.icu_admission_datetime,
		d.icu_discharge_datetime
	FROM (
		SELECT DISTINCT
			person_id,
			visit_occurrence_id,
			new_visit_detail_id,
			icu_admission_datetime,
			icu_discharge_datetime
		FROM icu_admission_details_multiple_visits
	) d
	INNER JOIN hic_cc_004.person p ON d.person_id = p.person_id
	INNER JOIN hic_cc_004.concept c ON p.gender_concept_id = c.concept_id
	)

	select * from icu_admission_details

----- Get indvidiual visit details
select
--*
distinct person_id
from hic_cc_004.visit_detail vd
where visit_detail_concept_id = 581379
and 	 COALESCE(vd.visit_detail_start_datetime, vd.visit_detail_start_date) >= '2021-12-01'
AND COALESCE(vd.visit_detail_start_datetime, vd.visit_detail_start_date) <= '2021-12-31'
--order by visit_detail_start_datetime

--- Caboodle ICU stays from the data mart
select
*
-- distinct icu.PatientDurableKey, icu.CrossIcuStayStartInstant, icu.CrossIcuStayEndInstant
from dbo.ICUStayRegistryDataMart icu
inner join dbo.DepartmentDim d
on icu.DepartmentKey = d.DepartmentKey
where icu.IcuStayStartInstant >= '2021-12-01'
and icu.IcuStayStartInstant <= '2021-12-31'

--CrossIcuStayStartInstant >= '2021-12-01'
--and CrossIcuStayStartInstant <= '2021-12-31'
and d.DepartmentStandardSpecialty <> 'Neonatology'
order by CrossIcuStayStartInstant

--- Caboodle to OMOP loading template script with critical care search.
SELECT enc.PatientDurableKey
	,PLF.EncounterKey
	,enc.DATE
	,enc.EndInstant AS EncounterEnd
	,plf.EventType
	,plf.StartInstant
	,plf.StartDateKey
	,plf.EndDateKey
	,plf.EndInstant
	,plf.DepartmentKey
	,D.[Name] AS CurrentDepartmentName
	,dx.DepartmentSpecialtyCode AS CurrentDepartmentSpecialty
	,hx.SpecialtyDepartmentName AS CurrentSpecialtyDepartmentName
	,hx.TreatmentFuncCodeDepSpecId AS CurrentTreatmentFuncCode
	,hx.TreatmentFuncCodeDepSpecName AS CurrentTreatmentFuncName
	,coalesce(dc.[DEPARTMENT_NAME], D.[Name]) AS DepartmentNameAtLocationEvent
	,coalesce(dc.SPECIALTY, dx.DepartmentSpecialtyCode) AS DepartmentSpecialtyAtLocationEvent
	,hx2.SpecialtyDepartmentName AS SpecialtyDepartmentNameAtLocationEvent
	,hx2.TreatmentFuncCodeDepSpecId AS TreatmentFuncCodeAtLocationEvent
	,hx2.TreatmentFuncCodeDepSpecName AS TreatmentFuncNameAtLocationEvent
	,ROW_NUMBER() OVER (PARTITION BY plf.EncounterKey ORDER BY StartInstant) as EncounterDetailNumber
FROM [FullAccess].[PatientLocationEventFact] AS PLF
INNER JOIN [DepartmentDim] AS d
	ON plf.[DepartmentKey] = d.[DepartmentKey]
INNER JOIN [DepartmentPlusDimX] AS dx
	ON plf.[DepartmentKey] = dx.[DepartmentKey]
LEFT JOIN ref.DepartmentChanges AS DC
	ON d.DepartmentEpicId = dc.DEPARTMENT_ID AND plf.StartInstant >= dc.START_DATE AND plf.StartInstant < ISNULL(dc.[END_DATE], '1 jan 2100')
INNER JOIN ReportingHierarchyMappingFactX hx2
	ON hx2.SpecialtyDepartmentId = coalesce(dc.SPECIALTY, dx.DepartmentSpecialtyCode) AND hx2.SpecialtyDepartmentId > 0
INNER JOIN ReportingHierarchyMappingFactX hx
	ON cast(hx.SpecialtyDepartmentId AS NVARCHAR) = dx.DepartmentSpecialtyCode AND hx.SpecialtyDepartmentId > 0
INNER JOIN EncounterFact enc
	ON enc.EncounterKey = plf.EncounterKey
WHERE
--	enc.PatientDurableKey IN ({pdks})
StartInstant >= '2021-12-01'
and StartInstant <= '2021-12-31'
and hx.SpecialtyDepartmentName like '%Critical Care%'
ORDER BY StartInstant
