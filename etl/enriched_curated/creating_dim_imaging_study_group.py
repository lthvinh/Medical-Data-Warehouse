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

    dim_imaging_studies__table_name = 'dim_imaging_studies'
    dim_imaging_studies__full_path = f'{curated_path}/{dim_imaging_studies__table_name}'
    bridge_imaging_study_group__table_name = 'bridge_imaging_study_group'
    bridge_imaging_study_group__full_path = f'{curated_path}/{bridge_imaging_study_group__table_name}'
    dim_imaging_study_group__table_name = 'dim_imaging_study_group'
    dim_imaging_study_group__full_path = f'{curated_path}/{dim_imaging_study_group__table_name}'
    enriched_encounters__table_name = 'enriched_encounters'
    enriched_encounters__full_path = f'{enriched_path}/{enriched_encounters__table_name}'
    enriched_encounters__df = spark.read.format('delta').option('path', enriched_encounters__full_path).load()
    enriched_imaging_studies__table_name = 'enriched_imaging_studies'
    enriched_imaging_studies__full_path = f'{enriched_path}/{enriched_imaging_studies__table_name}'
    enriched_imaging_studies__df = spark.read.format('delta').option('path', enriched_imaging_studies__full_path).load()
    
 
    dim_imaging_study_group__df = (
        enriched_encounters__df
        .select(
            F.sha2(F.col('ID'), 256).alias('Imaging_Study_Group_Key')
        )
        .dropDuplicates()
    )
    
    dim_imaging_study_group__is_existing = DeltaTable.isDeltaTable(spark, dim_imaging_study_group__full_path)
    if not dim_imaging_study_group__is_existing:
        dim_imaging_study_group__df.write.mode('overwrite').format('delta').option('path', dim_imaging_study_group__full_path).saveAsTable(dim_imaging_study_group__table_name)
    else:
        dim_imaging_study_group__delta_table = DeltaTable.forName(spark, dim_imaging_study_group__table_name)
        (
            dim_imaging_study_group__delta_table.alias('target').merge(
                source = dim_imaging_study_group__df.alias('source')
                , condition = (
                    (F.col('source.Imaging_Study_Group_Key') == F.col('target.Imaging_Study_Group_Key'))
                )
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
if __name__ == '__main__':
    main()