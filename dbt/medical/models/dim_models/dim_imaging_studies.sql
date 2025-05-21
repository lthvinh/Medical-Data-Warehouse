with dim_default_imaging_studies as(
	select
	    '0000000000000000000000000000000000000000000000000000000000000000' AS Imaging_Study_Key
	    , CAST(0 AS UInt64) AS Imaging_Study_Bodysite_Code
	    , 'Unknown' AS Imaging_Study_Bodysite_Description
	    , 'Unknown' AS Imaging_Study_Modality_Code
	    , 'Unknown' AS Imaging_Study_Modality_Description
)

 , dim_source_imaging_studies as (
 	select distinct
		CAST(BODYSITE_CODE AS UInt64)  as Imaging_Study_Bodysite_Code
	    , coalesce(BODYSITE_DESCRIPTION, 'Unknown') as Imaging_Study_Bodysite_Description
	    , coalesce(MODALITY_CODE, 'Unknown') as Imaging_Study_Modality_Code
	    , coalesce(MODALITY_DESCRIPTION, 'Unknown') as Imaging_Study_Modality_Description
	    
 	from
		{{ enriched_table('enriched_imaging_studies') }}
)
, hashed_key as (
	select
		hex(MD5(concat_ws('|', *))) as Imaging_Study_Key
		, *
	from
		dim_source_imaging_studies
)
, dim_imaging_studies as (
	select * from dim_default_imaging_studies
	union all
	select * from hashed_key
)

select * from dim_imaging_studies
order by Imaging_Study_Bodysite_Code

