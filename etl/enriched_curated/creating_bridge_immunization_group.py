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
    

    bridge_null_immunization_group__df = (
        enriched_encounters__df.alias('encounters')
        .join(
            enriched_immunizations__df.alias('immunizations')
            , (F.col('encounters.ID') == F.col('immunizations.ENCOUNTER'))
            , 'leftanti'
        )
        .select(
            F.sha2(F.col('ID'), 256).alias('Immunization_Group_Key')
            , F.lit('0000000000000000000000000000000000000000000000000000000000000000').alias('Immunization_Key')
        )
        .dropDuplicates()
    )
    
    bridge_immunization_group__df = (
        enriched_immunizations__df
        .select(
            F.sha2(F.col('ENCOUNTER'), 256).alias('Immunization_Group_Key')
            , F.sha2(
                F.concat_ws(
                    '|'
                    , F.ifnull(F.col('CODE'), F.lit(0))
                    , F.ifnull(F.col('DESCRIPTION'), F.lit('Unknown'))
                    , F.ifnull(F.col('BASE_COST'), F.lit(0.0))
                )
                , 256
            ).alias('Immunization_Key')
        )
        .dropDuplicates()
        .unionAll(bridge_null_immunization_group__df)
        .orderBy(F.col('Immunization_Group_Key'))
    )
    
    bridge_immunization_group__is_existing = DeltaTable.isDeltaTable(spark, bridge_immunization_group__full_path)
    if not bridge_immunization_group__is_existing:
        bridge_immunization_group__df.write.mode('overwrite').format('delta').option('path', bridge_immunization_group__full_path).saveAsTable(bridge_immunization_group__table_name)
    else:
        bridge_immunization_group__delta_table = DeltaTable.forName(spark, bridge_immunization_group__table_name)
        
        bridge_immunization_group__deleted_rows = (
            bridge_immunization_group__delta_table.toDF().where((F.col('Immunization_Key') == '0000000000000000000000000000000000000000000000000000000000000000')).alias('target')
            .join(
                bridge_immunization_group__df.alias('source')
                , (F.col('source.Immunization_Group_Key') == F.col('target.Immunization_Group_Key'))
                , 'leftsemi'
            )
        )
        
        (
            bridge_immunization_group__delta_table.alias('target').merge(
                source = bridge_immunization_group__deleted_rows.alias('source')
                , condition = (
                    (F.col('source.Immunization_Group_Key') == F.col('target.Immunization_Group_Key'))
                )
            )
            .whenMatchedDelete()
            .execute()
        )
    
        (
            bridge_immunization_group__delta_table.alias('target').merge(
                source = bridge_immunization_group__df.alias('source')
                , condition = (
                    (F.col('source.Immunization_Group_Key') == F.col('target.Immunization_Group_Key'))
                    & (F.col('source.Immunization_Key') == F.col('target.Immunization_Key'))
                )
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
    

        
if __name__ == '__main__':
    main()