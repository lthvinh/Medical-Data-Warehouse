from pyspark.sql import SparkSession
from pyspark.sql import types as T
from pyspark.sql import functions as F
from delta import *
import yaml

def main():
    with open('/opt/etl/config.yaml') as config_file:
        configs = yaml.safe_load(config_file)

    paths_configs = configs['paths']
    raw_path = paths_configs['raw']
    enriched_path = paths_configs['enriched']
    curated_path= paths_configs['curated']
    
    metastore_configs = configs['metastore']
    enriched_database = metastore_configs['enriched']
    curated_database = metastore_configs['curated']
    
    spark_configs = configs['spark']
    spark_config =  spark_configs['config']
    spark_packages = spark_configs['packages']
    
    spark_session_builder = (
        SparkSession
        .builder
        .config(map = spark_config)
    )
    
    spark = configure_spark_with_delta_pip(
        spark_session_builder = spark_session_builder
        , extra_packages = spark_packages
    ).getOrCreate()

    spark.catalog.setCurrentDatabase(curated_database)

    fact_encounters__table_name = 'fact_encounters'
    fact_encounters__full_path = f'{curated_path}/{fact_encounters__table_name}'
    dim_patients__table_name = 'dim_patients'
    dim_patients__full_path = f'{curated_path}/{dim_patients__table_name}'
    dim_patients__df = spark.read.format('delta').option('path', dim_patients__full_path).load().where((F.col('Is_Current'))).select(F.col('Patient_Key'), F.col('Patient_Id'))
    dim_organizations__table_name = 'dim_organizations'
    dim_organizations__full_path = f'{curated_path}/{dim_organizations__table_name}'
    dim_organizations__df = spark.read.format('delta').option('path', dim_organizations__full_path).load().where((F.col('Is_Current'))).select(F.col('Organization_Key'), F.col('Organization_Id'))
    dim_providers__table_name = 'dim_providers'
    dim_providers__full_path = f'{curated_path}/{dim_providers__table_name}'
    dim_providers__df = spark.read.format('delta').option('path', dim_providers__full_path).load().where((F.col('Is_Current'))).select(F.col('Provider_Key'), F.col('Provider_Id'))
    dim_payers__table_name = 'dim_payers'
    dim_payers__full_path = f'{curated_path}/{dim_payers__table_name}'
    dim_payers__df = spark.read.format('delta').option('path', dim_payers__full_path).load().where((F.col('Is_Current'))).select(F.col('Payer_Key'), F.col('Payer_Id'))
    enriched_encounters__table_name = 'enriched_encounters'
    enriched_encounters__full_path = f'{enriched_path}/{enriched_encounters__table_name}'
    enriched_encounters__df = spark.read.format('delta').option('path', enriched_encounters__full_path).load()
    
    fact_encounters__df = (
        enriched_encounters__df
        .select(
            F.col('ID').alias('Encounter_Id')
            , F.sha2(F.col('ID'), 256).alias('Hash_Id')
            , F.ifnull(F.col('START'), F.make_timestamp(years=F.lit(9999), months=F.lit(12), days=F.lit(31), hours=F.lit(0), mins=F.lit(0), secs=F.lit(0))).alias('START')
            , F.ifnull(F.col('STOP'), F.make_timestamp(years=F.lit(9999), months=F.lit(12), days=F.lit(31), hours=F.lit(0), mins=F.lit(0), secs=F.lit(0))).alias('STOP')
            , F.ifnull(F.col('PATIENT'), F.lit('00000000-0000-0000-0000-000000000000')).alias('PATIENT')
            , F.ifnull(F.col('ORGANIZATION'), F.lit('00000000-0000-0000-0000-000000000000')).alias('ORGANIZATION')
            , F.ifnull(F.col('PROVIDER'), F.lit('00000000-0000-0000-0000-000000000000')).alias('PROVIDER')
            , F.ifnull(F.col('PAYER'), F.lit('00000000-0000-0000-0000-000000000000')).alias('PAYER')
            , F.when(
                (F.col('CODE').isNotNull()) | (F.col('ENCOUNTERCLASS').isNotNull()) | (F.col('DESCRIPTION').isNotNull())
                , F.sha2(
                    F.concat_ws(
                        '|'
                        , F.ifnull(F.col('CODE'), F.lit(0))
                        , F.ifnull(F.col('ENCOUNTERCLASS'), F.lit('Unknown'))
                        , F.ifnull(F.col('DESCRIPTION'), F.lit('Unknown'))
                    )
                    , 256
                )
            ).otherwise('0000000000000000000000000000000000000000000000000000000000000000').alias('Encounter_Code_Key')
            , F.when(
                (F.col('REASONCODE').isNotNull()) | (F.col('REASONDESCRIPTION').isNotNull())
                , F.sha2(
                    F.concat_ws(
                        '|'
                        , F.ifnull(F.col('REASONCODE'), F.lit(0))
                        , F.ifnull(F.col('REASONDESCRIPTION'), F.lit('Unknown'))
                    )
                    , 256
                )
            ).otherwise('0000000000000000000000000000000000000000000000000000000000000000').alias('Reason_Code_Key')
            , F.ifnull(F.col('BASE_ENCOUNTER_COST'), F.lit(0.0)).alias('Base_Encounter_Cost')
            , F.ifnull(F.col('TOTAL_CLAIM_COST'), F.lit(0.0)).alias('Total_Claim_Cost')
            , F.ifnull(F.col('PAYER_COVERAGE'), F.lit(0.0)).alias('Payer_Coverage')
        ).alias('encounters')
        .join(dim_patients__df.alias('patients'), (F.col('encounters.PATIENT') == F.col('patients.Patient_Id')), 'left')
        .join(dim_organizations__df.alias('organizations'), (F.col('encounters.ORGANIZATION') == F.col('organizations.Organization_Id')), 'left')
        .join(dim_providers__df.alias('providers'), (F.col('encounters.PROVIDER') == F.col('providers.Provider_Id')), 'left')
        .join(dim_payers__df.alias('payers'), (F.col('encounters.PAYER') == F.col('payers.Payer_Id')), 'left')
        .select(
            F.col('encounters.Encounter_Id')
            , F.date_format(F.col('encounters.START'), 'yyyyMMdd').cast(T.IntegerType()).alias('Start_Date_Key')
            , F.date_format(F.col('encounters.START'), 'hhmmss').cast(T.IntegerType()).alias('Start_Time_Key')
            , F.date_format(F.col('encounters.STOP'), 'yyyyMMdd').cast(T.IntegerType()).alias('Stop_Date_Key')
            , F.date_format(F.col('encounters.STOP'), 'hhmmss').cast(T.IntegerType()).alias('Stop_Time_Key')
            , F.col('patients.Patient_Key').alias('Patient_Key')
            , F.col('organizations.Organization_Key').alias('Organization_Key')
            , F.col('providers.Provider_Key').alias('Provider_Key')
            , F.col('payers.Payer_Key').alias('Payer_Key')
            , F.col('encounters.Encounter_Code_Key')
            , F.col('encounters.Reason_Code_Key')
            , F.col('Hash_Id').alias('Allergy_Group_Key')
            , F.col('Hash_Id').alias('Care_Plan_Group_Key')
            , F.col('Hash_Id').alias('Condition_Group_Key')
            , F.col('Hash_Id').alias('Device_Group_Key')
            , F.col('Hash_Id').alias('Imaging_Study_Group_Key')
            , F.col('Hash_Id').alias('Immunization_Group_Key')
            , F.col('Hash_Id').alias('Medication_Group_Key')
            , F.col('Hash_Id').alias('Observation_Group_Key')
            , F.col('Hash_Id').alias('Procedure_Group_Key')
            , F.col('Hash_Id').alias('Supply_Group_Key')
            , F.col('Base_Encounter_Cost')
            , F.col('Total_Claim_Cost')
            , F.col('Payer_Coverage')
            , F.when(
                (F.col('START').isNotNull())
                & (F.col('STOP').isNotNull())
                , F.datediff(F.col('START'), F.col('STOP'))
            ).otherwise(-1).alias('Duration')
        )
    )
    
    fact_encounters__is_existing = DeltaTable.isDeltaTable(spark, fact_encounters__full_path)
    if not fact_encounters__is_existing:
        fact_encounters__df.write.mode('overwrite').format('delta').option('path', fact_encounters__full_path).saveAsTable(fact_encounters__table_name)
    else:
        fact_encounters__delta_table = DeltaTable.forName(spark, fact_encounters__table_name)
        (
            fact_encounters__delta_table.alias('target').merge(
                source = fact_encounters__df.alias('source')
                , condition = (
                    (F.col('source.Encounter_Id') == F.col('target.Encounter_Id'))
                )
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
if __name__ == '__main__':
    main()