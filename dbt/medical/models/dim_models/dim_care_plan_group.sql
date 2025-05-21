 WITH dim_care_plan_group AS (
 	SELECT
 		Encounter_Group_Key as Care_Plan_Group_Key
 	FROM	
 		{{ ref('_dim_encounter_group') }}
 )
 
SELECT * FROM dim_care_plan_group