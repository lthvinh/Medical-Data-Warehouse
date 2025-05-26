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

    dim_allergies__table_name = 'dim_allergies'
    dim_allergies__full_path = f'{curated_path}/{dim_allergies__table_name}'
    bridge_allergy_group__table_name = 'bridge_allergy_group'
    bridge_allergy_group__full_path = f'{curated_path}/{bridge_allergy_group__table_name}'
    dim_allergy_group__table_name = 'dim_allergy_group'
    dim_allergy_group__full_path = f'{curated_path}/{dim_allergy_group__table_name}'
    enriched_encounters__table_name = 'enriched_encounters'
    enriched_encounters__full_path = f'{enriched_path}/{enriched_encounters__table_name}'
    enriched_encounters__df = spark.read.format('delta').option('path', enriched_encounters__full_path).load()
    enriched_allergies__table_name = 'enriched_allergies'
    enriched_allergies__full_path = f'{enriched_path}/{enriched_allergies__table_name}'
    enriched_allergies__df = spark.read.format('delta').option('path', enriched_allergies__full_path).load()
    

    bridge_null_allergy_group__df = (
        enriched_encounters__df.alias('encounters')
        .join(
            enriched_allergies__df.alias('allergies')
            , (F.col('encounters.ID') == F.col('allergies.ENCOUNTER'))
            , 'leftanti'
        )
        .select(
            F.sha2(F.col('ID'), 256).alias('Allergy_Group_Key')
            , F.lit('0000000000000000000000000000000000000000000000000000000000000000').alias('Allergy_Key')
        )
        .dropDuplicates()
    )
    bridge_allergy_group__df = (
        enriched_allergies__df
        .select(
            F.sha2(F.col('ENCOUNTER'), 256).alias('Allergy_Group_Key')
            , F.sha2(
                F.concat_ws(
                    '|'
                    , F.ifnull(F.col('CODE'), F.lit(0))
                    , F.ifnull(F.col('SYSTEM'), F.lit('Unknown'))
                    , F.ifnull(F.col('DESCRIPTION'), F.lit('Unknown'))
                )
                , 256
            ).alias('Allergy_Key')
        )
        .dropDuplicates()
        .unionAll(bridge_null_allergy_group__df)
        .orderBy(F.col('Allergy_Group_Key'))
    )
    
    bridge_allergy_group__is_existing = DeltaTable.isDeltaTable(spark, bridge_allergy_group__full_path)
    if not bridge_allergy_group__is_existing:
        bridge_allergy_group__df.write.mode('overwrite').format('delta').option('path', bridge_allergy_group__full_path).saveAsTable(bridge_allergy_group__table_name)
    else:
        bridge_allergy_group__delta_table = DeltaTable.forName(spark, bridge_allergy_group__table_name)
        
        bridge_allergy_group__deleted_rows = (
            bridge_allergy_group__delta_table.toDF().where((F.col('Allergy_Key') == '0000000000000000000000000000000000000000000000000000000000000000')).alias('target')
            .join(
                bridge_allergy_group__df.alias('source')
                , (F.col('source.Allergy_Group_Key') == F.col('target.Allergy_Group_Key'))
                , 'leftsemi'
            )
        )
        
        (
            bridge_allergy_group__delta_table.alias('target').merge(
                source = bridge_allergy_group__deleted_rows.alias('source')
                , condition = (
                    (F.col('source.Allergy_Group_Key') == F.col('target.Allergy_Group_Key'))
                )
            )
            .whenMatchedDelete()
            .execute()
        )
    
        (
            bridge_allergy_group__delta_table.alias('target').merge(
                source = bridge_allergy_group__df.alias('source')
                , condition = (
                    (F.col('source.Allergy_Group_Key') == F.col('target.Allergy_Group_Key'))
                    & (F.col('source.Allergy_Key') == F.col('target.Allergy_Key'))
                )
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
        
if __name__ == '__main__':
    main()