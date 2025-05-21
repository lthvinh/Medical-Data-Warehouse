 WITH dim_immunization_group AS (
 	SELECT
 		Encounter_Group_Key as Immunization_Group_Key
 	FROM	
 		{{ ref('_dim_encounter_group') }}
 )
 
SELECT * FROM dim_immunization_group