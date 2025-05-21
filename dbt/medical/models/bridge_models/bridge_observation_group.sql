with enriched_encounters as (
	select
		ID
	from
		{{ enriched_table('enriched_encounters') }}
)
, enriched_observations as (
 	select distinct
 		ENCOUNTER
 		, coalesce(CODE, 'Unknown') as CODE
 	from
		{{ enriched_table('enriched_observations') }}
	
)
, dim_observations as (
	select
		Observation_Key
		, Observation_Code 
	from
		{{ ref('dim_observations') }}
)

, encounters_without_observation as (
	select distinct 
		ID as ENCOUNTER
		, 'Unknown' as CODE
	from 
		enriched_encounters as e
	left anti join 
		enriched_observations as a on e.ID = a.ENCOUNTER
)

, observation_group_union as(
	select * from enriched_observations
	union all
	select * from encounters_without_observation
)
, bridge_observation_group as (
	select
		hex(MD5(ENCOUNTER)) as Observation_Group_Key
		, Observation_Key
	from
		observation_group_union as ag
	join
		dim_observations as da on ag.CODE == da.Observation_Code
)

select * from bridge_observation_group


