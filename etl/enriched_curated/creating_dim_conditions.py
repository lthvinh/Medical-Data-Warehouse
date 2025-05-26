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


    dim_conditions__table_name = 'dim_conditions'
    dim_conditions__full_path = f'{curated_path}/{dim_conditions__table_name}'
    bridge_condition_group__table_name = 'bridge_condition_group'
    bridge_condition_group__full_path = f'{curated_path}/{bridge_condition_group__table_name}'
    dim_condition_group__table_name = 'dim_condition_group'
    dim_condition_group__full_path = f'{curated_path}/{dim_condition_group__table_name}'
    enriched_encounters__table_name = 'enriched_encounters'
    enriched_encounters__full_path = f'{enriched_path}/{enriched_encounters__table_name}'
    enriched_encounters__df = spark.read.format('delta').option('path', enriched_encounters__full_path).load()
    enriched_conditions__table_name = 'enriched_conditions'
    enriched_conditions__full_path = f'{enriched_path}/{enriched_conditions__table_name}'
    enriched_conditions__df = spark.read.format('delta').option('path', enriched_conditions__full_path).load()
    

    dim_default_conditions__df = spark.sql('''
        SELECT
            '0000000000000000000000000000000000000000000000000000000000000000' AS Condition_Key
            , CAST(0 AS LONG) AS Condition_Code
            , 'Unknown' AS Condition_Description
    ''')
    
    dim_conditions__df = (
        enriched_conditions__df
        .select(
            F.ifnull(F.col('CODE'), F.lit(0)).alias('Condition_Code')
            , F.ifnull(F.col('DESCRIPTION'), F.lit('Unknown')).alias('Condition_Description')
        )
        .dropDuplicates()
        .select(
            F.sha2(
                F.concat_ws(
                    '|'
                    , F.col('Condition_Code')
                    , F.col('Condition_Description')
                )
                , 256
            ).alias('Condition_Key')
            , F.col('Condition_Code')
            , F.col('Condition_Description')
        )
        .unionAll(dim_default_conditions__df)
        .orderBy(F.col('Condition_Code').asc())
    )
    
    dim_conditions__is_existing = DeltaTable.isDeltaTable(spark, dim_conditions__full_path)
    if not dim_conditions__is_existing:
        dim_conditions__df.write.mode('overwrite').format('delta').option('path', dim_conditions__full_path).saveAsTable(dim_conditions__table_name)
    else:
        dim_conditions__delta_table = DeltaTable.forName(spark, dim_conditions__table_name)
        (
            dim_conditions__delta_table.alias('target').merge(
                source = dim_conditions__df.alias('source')
                , condition = (
                    (F.col('source.Condition_Key') == F.col('target.Condition_Key'))     
                )
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
    
    


if __name__ == '__main__':
    main()