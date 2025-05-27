{{
	config(
		materialized = "incremental"
		, incremental_strategy = "append"
		, engine = "MergeTree()"
		, unique_key = "(Imaging_Study_Key)"
	)
}}

WITH dim_imaging_study_group AS (
 	select
 		Encounter_Group_Key as Imaging_Study_Key
 	from	
 		{{ ref('_dim_encounter_group') }}
 )
 
select * from dim_imaging_study_group

{% if is_incremental() %}

left join {{ this }} as target
	on source.Imaging_Study_Key = target.Imaging_Study_Key
where
	target.Imaging_Study_Key is null

{% endif %}