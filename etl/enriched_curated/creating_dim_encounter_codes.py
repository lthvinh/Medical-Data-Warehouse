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

    dim_encounter_codes__table_name = 'dim_encounter_codes'
    enriched_encounters__table_name = 'enriched_encounters'
    enriched_encoutners__full_path = f'{enriched_path}/{enriched_encounters__table_name}'
    enriched_encounters__df = spark.read.format('delta').option('path', enriched_encoutners__full_path).load()
    dim_encoutner_codes__full_path = f'{curated_path}/{dim_encounter_codes__table_name}'
    
    dim_default_encounter_codes__df = spark.sql('''
        SELECT 
            '0000000000000000000000000000000000000000000000000000000000000000' AS Encounter_Code_Key
            , CAST(0 AS LONG) AS Encounter_Code
            , 'Unknown' AS Encounter_Class
            , 'Unknown' AS Encounter_Description
    ''')
    
    dim_encounter_codes__df = (
        enriched_encounters__df
        .where(
            (F.col('CODE').isNotNull())
            | (F.col('ENCOUNTERCLASS').isNotNull())
            | (F.col('DESCRIPTION').isNotNull())
        )
        .select(
            F.ifnull(F.col('CODE'), F.lit(0)).alias('Encounter_Code')
            , F.ifnull(F.col('ENCOUNTERCLASS'), F.lit('Unknown')).alias('Encounter_Class')
            , F.ifnull(F.col('DESCRIPTION'), F.lit('Unknown')).alias('Encounter_Description')
        )
        .dropDuplicates()
        .select(
            F.sha2(
                F.concat_ws('|', F.col('Encounter_Code'), F.col('Encounter_Class'), F.col('Encounter_Description'))
                , 256
            ).alias('Encounter_Code_Key')
            , F.col('Encounter_Code')
            , F.col('Encounter_Class')
            , F.col('Encounter_Description')
        )
        .unionAll(dim_default_encounter_codes__df)
        .orderBy(F.col('Encounter_Code').asc())
    )
    
    dim_encounter_codes__is_existing = DeltaTable.isDeltaTable(spark, dim_encoutner_codes__full_path)
    if not dim_encounter_codes__is_existing:
        dim_encounter_codes__df.write.mode('overwrite').format('delta').option('path', dim_encoutner_codes__full_path).saveAsTable(dim_encounter_codes__table_name)
    else:
        dim_encoutner_codes__delta_table = DeltaTable.forName(spark, dim_encounter_codes__table_name)
        (
            dim_encoutner_codes__delta_table.alias('target').merge(
                source = dim_encounter_codes__df.alias('source')
                , condition = (F.col('source.Encounter_Code_Key') == F.col('target.Encounter_Code_Key'))
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
        
if __name__ == '__main__':
    main()