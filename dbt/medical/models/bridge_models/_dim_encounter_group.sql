{{
	config(
		materialized = 'ephemeral'
	)
}}

with encounter_group_keys as (
	select
		hex(MD5(ID)) as Encounter_Group_Key
	from
		{{ enriched_table('enriched_encounters') }}
)

select * from encounter_group_keys