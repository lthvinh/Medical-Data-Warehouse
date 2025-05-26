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

    dim_observations__table_name = 'dim_observations'
    dim_observations__full_path = f'{curated_path}/{dim_observations__table_name}'
    bridge_observation_group__table_name = 'bridge_observation_group'
    bridge_observation_group__full_path = f'{curated_path}/{bridge_observation_group__table_name}'
    dim_observation_group__table_name = 'dim_observation_group'
    dim_observation_group__full_path = f'{curated_path}/{dim_observation_group__table_name}'
    enriched_encounters__table_name = 'enriched_encounters'
    enriched_encounters__full_path = f'{enriched_path}/{enriched_encounters__table_name}'
    enriched_encounters__df = spark.read.format('delta').option('path', enriched_encounters__full_path).load()
    enriched_observations__table_name = 'enriched_observations'
    enriched_observations__full_path = f'{enriched_path}/{enriched_observations__table_name}'
    enriched_observations__df = spark.read.format('delta').option('path', enriched_observations__full_path).load()
    
    # # DIM OBSERVATIONS---------------------------------------------
    dim_default_observations__df = spark.sql('''
        SELECT
            '0000000000000000000000000000000000000000000000000000000000000000' AS Observation_Key
            , CAST(0 AS LONG) AS Observation_Code
            , 'Unknown' AS Observation_Category
            , 'Unknown' AS Observation_Description
            
    ''')
    
    
    dim_observations__df = (
        enriched_observations__df
        .select(
            F.ifnull(F.col('CODE'), F.lit(0)).alias('Observation_Code')
            , F.ifnull(F.col('CATEGORY'), F.lit('Unknown')).alias('Observation_Category')
            , F.ifnull(F.col('DESCRIPTION'), F.lit('Unknown')).alias('Observation_Description')
        )
        .dropDuplicates()
        .select(
            F.sha2(
                F.concat_ws(
                    '|'
                    , F.col('Observation_Code')
                    , F.col('Observation_Category')
                    , F.col('Observation_Description')
                )
                , 256
            ).alias('Observation_Key')
            , F.col('Observation_Code')
            , F.col('Observation_Category')
            , F.col('Observation_Description')
        )
        .unionAll(dim_default_observations__df)
        .orderBy(F.col('Observation_Code').asc())
    )
    
    dim_observations__is_existing = DeltaTable.isDeltaTable(spark, dim_observations__full_path)
    if not dim_observations__is_existing:
        dim_observations__df.write.mode('overwrite').format('delta').option('path', dim_observations__full_path).saveAsTable(dim_observations__table_name)
    else:
        dim_observations__delta_table = DeltaTable.forName(spark, dim_observations__table_name)
        (
            dim_observations__delta_table.alias('target').merge(
                source = dim_observations__df.alias('source')
                , condition = (
                    (F.col('source.Observation_Key') == F.col('target.Observation_Key'))     
                )
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
    
    

        
if __name__ == '__main__':
    main()