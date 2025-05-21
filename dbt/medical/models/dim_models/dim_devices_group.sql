 WITH dim_device_group AS (
 	SELECT
 		Encounter_Group_Key as Device_Group_Key
 	FROM	
 		{{ ref('_dim_encounter_group') }}
 )
 
SELECT * FROM dim_device_group