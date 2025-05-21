 WITH dim_supply_group AS (
 	SELECT
 		Encounter_Group_Key as Supply_Group_Key
 	FROM	
 		{{ ref('_dim_encounter_group') }}
 )
 
SELECT * FROM dim_supply_group