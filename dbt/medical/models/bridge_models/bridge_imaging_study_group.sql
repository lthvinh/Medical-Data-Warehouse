with enriched_encounters as (
	select
		ID
	from
    	{{ enriched_table('enriched_encounters') }}

)
, enriched_imaging_studies as (
 	select distinct
 		ENCOUNTER
 		, cast(coalesce(BODYSITE_CODE, 0) as UInt64) as CODE
 	from
    	{{ enriched_table('enriched_imaging_studies') }}

	
)
, dim_imaging_studies as (
	select
		Imaging_Study_Key
		, Imaging_Study_Bodysite_Code 
	from
		{{ ref('dim_imaging_studies') }}
)

, encounters_without_imaging_study as (
	select distinct 
		ID as ENCOUNTER
		, cast(0 as UInt64) as CODE
	from 
		enriched_encounters as e
	left anti join 
		enriched_imaging_studies as a on e.ID = a.ENCOUNTER
)

, imaging_study_group_union as(
	select * from enriched_imaging_studies
	union all
	select * from encounters_without_imaging_study
)

, imaging_study_group as (
	select
		hex(MD5(ENCOUNTER)) as Imaging_Group_Key
		, Imaging_Study_Key
	from
		imaging_study_group_union as ag
	join
		dim_imaging_studies as da on ag.CODE == da.Imaging_Study_Bodysite_Code
)

select * from imaging_study_group