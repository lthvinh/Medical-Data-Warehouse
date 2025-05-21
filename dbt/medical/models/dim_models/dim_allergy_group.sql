WITH dim_allergy_group AS (
 	SELECT
 		Encounter_Group_Key as Allergy_Group_Key
 	FROM	
 		{{ ref('_dim_encounter_group') }}
)
 
SELECT * FROM dim_allergy_group