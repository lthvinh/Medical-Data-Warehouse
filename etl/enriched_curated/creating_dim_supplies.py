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

    dim_supplies__table_name = 'dim_supplies'
    dim_supplies__full_path = f'{curated_path}/{dim_supplies__table_name}'
    bridge_supply_group__table_name = 'bridge_supply_group'
    bridge_supply_group__full_path = f'{curated_path}/{bridge_supply_group__table_name}'
    dim_supply_group__table_name = 'dim_supply_group'
    dim_supply_group__full_path = f'{curated_path}/{dim_supply_group__table_name}'
    enriched_encounters__table_name = 'enriched_encounters'
    enriched_encounters__full_path = f'{enriched_path}/{enriched_encounters__table_name}'
    enriched_encounters__df = spark.read.format('delta').option('path', enriched_encounters__full_path).load()
    enriched_supplies__table_name = 'enriched_supplies'
    enriched_supplies__full_path = f'{enriched_path}/{enriched_supplies__table_name}'
    enriched_supplies__df = spark.read.format('delta').option('path', enriched_supplies__full_path).load()
    
    # # DIM SUPPLIES---------------------------------------------
    dim_default_supplies__df = spark.sql('''
        SELECT
            '0000000000000000000000000000000000000000000000000000000000000000' AS Supply_Key
            , CAST(0 AS LONG) AS Supply_Code
            , 'Unknown' AS Supply_Description
            , CAST(0 AS LONG) AS Supply_Quantity
    ''')
    
    dim_supplies__df = (
        enriched_supplies__df
        .select(
            F.ifnull(F.col('CODE'), F.lit(0)).alias('Supply_Code')
            , F.ifnull(F.col('DESCRIPTION'), F.lit('Unknown')).alias('Supply_Description')
            , F.ifnull(F.col('QUANTITY'), F.lit(0)).alias('Supply_Quantity')
        )
        .dropDuplicates()
        .select(
            F.sha2(
                F.concat_ws(
                    '|'
                    , F.col('Supply_Code')
                    , F.col('Supply_Description')
                    , F.col('Supply_Quantity')
                )
                , 256
            ).alias('Supply_Key')
            , F.col('Supply_Code')
            , F.col('Supply_Description')
            , F.col('Supply_Quantity')
        )
        .unionAll(dim_default_supplies__df)
        .orderBy(F.col('Supply_Code').asc())
    )
    
    dim_supplies__is_existing = DeltaTable.isDeltaTable(spark, dim_supplies__full_path)
    if not dim_supplies__is_existing:
        dim_supplies__df.write.mode('overwrite').format('delta').option('path', dim_supplies__full_path).saveAsTable(dim_supplies__table_name)
    else:
        dim_supplies__delta_table = DeltaTable.forName(spark, dim_supplies__table_name)
        (
            dim_supplies__delta_table.alias('target').merge(
                source = dim_supplies__df.alias('source')
                , condition = (
                    (F.col('source.Supply_Key') == F.col('target.Supply_Key'))     
                )
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
    
    
if __name__ == '__main__':
    main()