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

    dim_immunizations__table_name = 'dim_immunizations'
    dim_immunizations__full_path = f'{curated_path}/{dim_immunizations__table_name}'
    bridge_immunization_group__table_name = 'bridge_immunization_group'
    bridge_immunization_group__full_path = f'{curated_path}/{bridge_immunization_group__table_name}'
    dim_immunization_group__table_name = 'dim_immunization_group'
    dim_immunization_group__full_path = f'{curated_path}/{dim_immunization_group__table_name}'
    enriched_encounters__table_name = 'enriched_encounters'
    enriched_encounters__full_path = f'{enriched_path}/{enriched_encounters__table_name}'
    enriched_encounters__df = spark.read.format('delta').option('path', enriched_encounters__full_path).load()
    enriched_immunizations__table_name = 'enriched_immunizations'
    enriched_immunizations__full_path = f'{enriched_path}/{enriched_immunizations__table_name}'
    enriched_immunizations__df = spark.read.format('delta').option('path', enriched_immunizations__full_path).load()
    
    # # DIM IMMUNIZATIONS---------------------------------------------
    dim_default_immunizations__df = spark.sql('''
        SELECT
            '0000000000000000000000000000000000000000000000000000000000000000' AS Immunization_Key
            , CAST(0 AS LONG) AS Immunization_Code
            , 'Unknown' AS Immunization_Description
            , CAST(0.0 AS DOUBLE) AS Immunization_Base_Cost
    ''')
    
    
    dim_immunizations__df = (
        enriched_immunizations__df
        .select(
            F.ifnull(F.col('CODE'), F.lit(0)).alias('Immunization_Code')
            , F.ifnull(F.col('DESCRIPTION'), F.lit('Unknown')).alias('Immunization_Description')
            , F.ifnull(F.col('BASE_COST'), F.lit(0.0)).alias('Immunization_Base_Cost')
        )
        .dropDuplicates()
        .select(
            F.sha2(
                F.concat_ws(
                    '|'
                    , F.col('Immunization_Code')
                    , F.col('Immunization_Description')
                    , F.col('Immunization_Base_Cost')
                )
                , 256
            ).alias('Immunization_Key')
            , F.col('Immunization_Code')
            , F.col('Immunization_Description')
            , F.col('Immunization_Base_Cost')
        )
        .unionAll(dim_default_immunizations__df)
        .orderBy(F.col('Immunization_Code').asc())
    )
    
    dim_immunizations__is_existing = DeltaTable.isDeltaTable(spark, dim_immunizations__full_path)
    if not dim_immunizations__is_existing:
        dim_immunizations__df.write.mode('overwrite').format('delta').option('path', dim_immunizations__full_path).saveAsTable(dim_immunizations__table_name)
    else:
        dim_immunizations__delta_table = DeltaTable.forName(spark, dim_immunizations__table_name)
        (
            dim_immunizations__delta_table.alias('target').merge(
                source = dim_immunizations__df.alias('source')
                , condition = (
                    (F.col('source.Immunization_Key') == F.col('target.Immunization_Key'))     
                )
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
    
    
  
        
if __name__ == '__main__':
    main()