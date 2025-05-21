 WITH dim_observation_group AS (
 	SELECT
 		Encounter_Group_Key as Obsercation_Group_Key
 	FROM	
 		{{ ref('_dim_encounter_group') }}
 )
 
SELECT * FROM dim_observation_group