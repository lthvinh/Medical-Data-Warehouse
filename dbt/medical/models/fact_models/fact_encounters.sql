
with enriched_encounters as (
select
    ID as Encounter_Id
    , hex(MD5(ID)) as Encounter_Group_Key
    , coalesce(START, toDateTime('2100-01-01 00:00:00')) as Start
    , coalesce(STOP, toDateTime('2100-01-01 00:00:00')) as Stop
    , coalesce(PATIENT, '00000000-0000-0000-0000-000000000000') as  Patient_Id
    , coalesce(ORGANIZATION, '00000000-0000-0000-0000-000000000000') as  Organization_Id
    , coalesce(PROVIDER, '00000000-0000-0000-0000-000000000000') as  Provider_Id
    , coalesce(PAYER, '00000000-0000-0000-0000-000000000000') as  Payer_Id
    , if(
        isNotNull(CODE) or isNotNull(ENCOUNTERCLASS) or isNotNull(DESCRIPTION)
        , hex(MD5(concat_ws('|', CODE, ENCOUNTERCLASS, DESCRIPTION)))
        , '0000000000000000000000000000000000000000000000000000000000000000'
    ) as Encounter_Code_Key
    , if(
        isNotNull(REASONCODE) or isNotNull(REASONDESCRIPTION)
        , hex(MD5(concat_ws('|', REASONCODE, REASONDESCRIPTION, DESCRIPTION)))
        , '0000000000000000000000000000000000000000000000000000000000000000'
    ) as Reason_Code_Key
    , cast(coalesce(BASE_ENCOUNTER_COST, 0.0) as Decimal32(2)) as Base_Encounter_Cost
    , cast(coalesce(TOTAL_CLAIM_COST, 0.0) as Decimal32(2)) as Total_Claim_Cost
    , cast(coalesce(PAYER_COVERAGE, 0.0) as Decimal32(2)) as Payer_Coverage
        
from 	
    {{ enriched_table('enriched_encounters') }}
)
, dim_patients as (
select
    Patient_Key
    , Patient_Id
from
    {{ ref('dim_patients') }}
)
, dim_organizations as (
select
    Organization_Key
    , Organization_Id
from
    {{ ref('dim_organizations') }}
)
, dim_providers as (
select
    Provider_Key
    , Provider_Id
from
    {{ ref('dim_providers') }}

)
, dim_payers as (
select
    Payer_Key
    , Payer_Id
from
    {{ ref('dim_payers') }}
)
, fact_encounters as (
select
    enc.Encounter_Id
    , coalesce(toYYYYMMDD(enc.Start), 21000101) as Start_Date_Key
    , cast(date_format(enc.Start, '%H%m%S') as UInt32) as Start_Time_Key
    , coalesce(toYYYYMMDD(enc.Stop), 21000101) as Stop_Date_Key
    , cast(date_format(enc.Stop, '%H%m%S') as UInt32) as Stop_Time_Key
    , pat.Patient_Key
    , org.Organization_Key
    , pro.Provider_Key
    , pay.Payer_Key
    , enc.Encounter_Code_Key
    , enc.Reason_Code_Key
    , enc.Encounter_Group_Key as Allergy_Group_Key
    , enc.Encounter_Group_Key as Care_Plan_Group_Key
    , enc.Encounter_Group_Key as Condition_Group_Key
    , enc.Encounter_Group_Key as Device_Group_Key
    , enc.Encounter_Group_Key as Imaging_Study_Group_Key
    , enc.Encounter_Group_Key as Immunization_Group_Key
    , enc.Encounter_Group_Key as Medication_Group_Key
    , enc.Encounter_Group_Key as Observation_Group_Key
    , enc.Encounter_Group_Key as Procedure_Group_Key
    , enc.Encounter_Group_Key as Supply_Group_Key
    , enc.Base_Encounter_Cost
    , enc.Total_Claim_Cost
    , enc.Payer_Coverage
    , if(isNotNull(Start) and isNotNull(Stop), timeDiff(Start, Stop), 0) as Duration
from
    enriched_encounters as enc
left join
    dim_patients as pat on enc.Patient_Id = pat.Patient_Id
left join
    dim_organizations as org on enc.Organization_Id = org.Organization_Id
left join
    dim_providers as pro on enc.Provider_Id = pro.Provider_Id
left join
    dim_payers as pay on enc.Payer_Id = pay.Payer_Id
)

select * from fact_encounters

