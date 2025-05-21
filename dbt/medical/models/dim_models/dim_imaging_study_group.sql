 WITH dim_imaging_study_group AS (
 	SELECT
 		Encounter_Group_Key as Imaging_Study_Key
 	FROM	
 		{{ ref('_dim_encounter_group') }}
 )
 
SELECT * FROM dim_imaging_study_group