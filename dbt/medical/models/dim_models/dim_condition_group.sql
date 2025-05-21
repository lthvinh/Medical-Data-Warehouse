 WITH dim_condition_group AS (
 	SELECT
 		Encounter_Group_Key as Condition_Group_Key
 	FROM	
 		{{ ref('_dim_encounter_group') }}
 )
 
SELECT * FROM dim_condition_group