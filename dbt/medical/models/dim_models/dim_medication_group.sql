 WITH dim_medication_group AS (
 	SELECT
 		Encounter_Group_Key as Medication_Group_key
 	FROM	
 		{{ ref('_dim_encounter_group') }}
 )
 
SELECT * FROM dim_medication_group