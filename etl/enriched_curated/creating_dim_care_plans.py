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

    dim_care_plans__table_name = 'dim_care_plans'
    dim_care_plans__full_path = f'{curated_path}/{dim_care_plans__table_name}'
    bridge_care_plan_group__table_name = 'bridge_care_plan_group'
    bridge_care_plan_group__full_path = f'{curated_path}/{bridge_care_plan_group__table_name}'
    dim_care_plan_group__table_name = 'dim_care_plan_group'
    dim_care_plan_group__full_path = f'{curated_path}/{dim_care_plan_group__table_name}'
    enriched_encounters__table_name = 'enriched_encounters'
    enriched_encounters__full_path = f'{enriched_path}/{enriched_encounters__table_name}'
    enriched_encounters__df = spark.read.format('delta').option('path', enriched_encounters__full_path).load()
    enriched_care_plans__table_name = 'enriched_careplans'
    enriched_care_plans__full_path = f'{enriched_path}/{enriched_care_plans__table_name}'
    enriched_care_plans__df = spark.read.format('delta').option('path', enriched_care_plans__full_path).load()
    
    # # DIM CARE PLANS---------------------------------------------
    dim_default_care_plans__df = spark.sql('''
        SELECT
            '0000000000000000000000000000000000000000000000000000000000000000' AS Care_Plan_Key
            , CAST(0 AS LONG) AS Care_Plan_Code
            , 'Unknown' AS Care_Plan_Description
    ''')
    
    dim_care_plans__df = (
        enriched_care_plans__df
        .select(
            F.ifnull(F.col('CODE'), F.lit(0)).alias('Care_Plan_Code')
            , F.ifnull(F.col('DESCRIPTION'), F.lit('Unknown')).alias('Care_Plan_Description')
        )
        .dropDuplicates()
        .select(
            F.sha2(
                F.concat_ws(
                    '|'
                    , F.col('Care_Plan_Code')
                    , F.col('Care_Plan_Description')
                )
                , 256
            ).alias('Care_Plan_Key')
            , F.col('Care_Plan_Code')
            , F.col('Care_Plan_Description')
        )
        .unionAll(dim_default_care_plans__df)
        .orderBy(F.col('Care_Plan_Code').asc())
    )
    
    dim_care_plans__is_existing = DeltaTable.isDeltaTable(spark, dim_care_plans__full_path)
    if not dim_care_plans__is_existing:
        dim_care_plans__df.write.mode('overwrite').format('delta').option('path', dim_care_plans__full_path).saveAsTable(dim_care_plans__table_name)
    else:
        dim_care_plans__delta_table = DeltaTable.forName(spark, dim_care_plans__table_name)
        (
            dim_care_plans__delta_table.alias('target').merge(
                source = dim_care_plans__df.alias('source')
                , condition = (
                    (F.col('source.Care_Plan_Key') == F.col('target.Care_Plan_Key'))     
                )
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
    
    
    # BRIDAGE CARE PLAN GROUP--------------------------------------------
    bridge_null_care_plan_group__df = (
        enriched_encounters__df.alias('encounters')
        .join(
            enriched_care_plans__df.alias('care_plans')
            , (F.col('encounters.ID') == F.col('care_plans.ENCOUNTER'))
            , 'leftanti'
        )
        .select(
            F.sha2(F.col('ID'), 256).alias('Care_Plan_Group_Key')
            , F.lit('0000000000000000000000000000000000000000000000000000000000000000').alias('Care_Plan_Key')
        )
        .dropDuplicates()
    )
    
    bridge_care_plan_group__df = (
        enriched_care_plans__df
        .select(
            F.sha2(F.col('ENCOUNTER'), 256).alias('Care_Plan_Group_Key')
            , F.sha2(
                F.concat_ws(
                    '|'
                    , F.ifnull(F.col('CODE'), F.lit(0)).alias('Care_Plan_Code')
                    , F.ifnull(F.col('DESCRIPTION'), F.lit('Unknown')).alias('Care_Plan_Description')
                )
                , 256
            ).alias('Care_Plan_Key')
        )
        .dropDuplicates()
        .unionAll(bridge_null_care_plan_group__df)
        .orderBy(F.col('Care_Plan_Group_Key'))
    )
    
    bridge_care_plan_group__is_existing = DeltaTable.isDeltaTable(spark, bridge_care_plan_group__full_path)
    if not bridge_care_plan_group__is_existing:
        bridge_care_plan_group__df.write.mode('overwrite').format('delta').option('path', bridge_care_plan_group__full_path).saveAsTable(bridge_care_plan_group__table_name)
    else:
        bridge_care_plan_group__delta_table = DeltaTable.forName(spark, bridge_care_plan_group__table_name)
        
        bridge_care_plan_group__deleted_rows = (
            bridge_care_plan_group__delta_table.toDF().where((F.col('Care_Plan_Key') == '0000000000000000000000000000000000000000000000000000000000000000')).alias('target')
            .join(
                bridge_care_plan_group__df.alias('source')
                , (F.col('source.Care_Plan_Group_Key') == F.col('target.Care_Plan_Group_Key'))
                , 'leftsemi'
            )
        )
        
        (
            bridge_care_plan_group__delta_table.alias('target').merge(
                source = bridge_care_plan_group__deleted_rows.alias('source')
                , condition = (
                    (F.col('source.Care_Plan_Group_Key') == F.col('target.Care_Plan_Group_Key'))
                )
            )
            .whenMatchedDelete()
            .execute()
        )
    
        (
            bridge_care_plan_group__delta_table.alias('target').merge(
                source = bridge_care_plan_group__df.alias('source')
                , condition = (
                    (F.col('source.Care_Plan_Group_Key') == F.col('target.Care_Plan_Group_Key'))
                    & (F.col('source.Care_Plan_Key') == F.col('target.Care_Plan_Key'))
                )
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
    
    # # DIM CARE PLAN GROUP--------------------------------------------
    dim_care_plan_group__df = (
        enriched_encounters__df
        .select(
            F.sha2(F.col('ID'), 256).alias('Care_Plan_Group_Key')
        )
        .dropDuplicates()
    )
    
    dim_care_plan_group__is_existing = DeltaTable.isDeltaTable(spark, dim_care_plan_group__full_path)
    if not dim_care_plan_group__is_existing:
        dim_care_plan_group__df.write.mode('overwrite').format('delta').option('path', dim_care_plan_group__full_path).saveAsTable(dim_care_plan_group__table_name)
    else:
        dim_care_plan_group__delta_table = DeltaTable.forName(spark, dim_care_plan_group__table_name)
        (
            dim_care_plan_group__delta_table.alias('target').merge(
                source = dim_care_plan_group__df.alias('source')
                , condition = (
                    (F.col('source.Care_Plan_Group_Key') == F.col('target.Care_Plan_Group_Key'))
                )
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
    
if __name__ == '__main__':
    main()