from pyspark.sql import SparkSession
from pyspark import SparkConf
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

    spark.catalog.setCurrentDatabase(enriched_database)

    raw_organizations__file_name = 'organizations.csv'
    enriched_organizations__table_name = 'enriched_organizations'
    raw_organizations__full_path = f'{raw_path}/{raw_organizations__file_name}'
    enriched_organizations__full_path = f'{enriched_path}/{enriched_organizations__table_name}'
    
    raw_organizations__schema = T.StructType([
        T.StructField('ID', T.StringType(), True)
        , T.StructField('NAME', T.StringType(), True)
        , T.StructField('ADDRESS', T.StringType(), True)
        , T.StructField('CITY', T.StringType(), True)
        , T.StructField('STATE', T.StringType(), True)
        , T.StructField('ZIP', T.LongType(), True)
        , T.StructField('LAT', T.DoubleType(), True)
        , T.StructField('LON', T.DoubleType(), True)
        , T.StructField('PHONE', T.StringType(), True)
        , T.StructField('REVENUE', T.DoubleType(), True)
        , T.StructField('UTILIZATION', T.LongType(), True)
    ])
    
    raw_organizations__df = (
        spark.read.format('csv')
        .options(**{
            'path': raw_organizations__full_path
            , 'header': True
            , 'inferSchema': False
        })
        .schema(raw_organizations__schema)
        .load()
        .dropDuplicates()
    )
    
    enriched_organizations__df = raw_organizations__df.withColumn(
        'HASH', F.concat_ws('|', *[F.col(column_name) for column_name in raw_organizations__df.columns])
    )
    
    enriched_organizations__is_existing = DeltaTable.isDeltaTable(spark, enriched_organizations__full_path)
    if not enriched_organizations__is_existing:
        enriched_organizations__df.write.format('delta').mode('overwrite').option('path', enriched_organizations__full_path).saveAsTable(enriched_organizations__table_name)
    else:
        enriched_organizations__delta_table = DeltaTable.forName(spark, enriched_organizations__table_name)
        (
            enriched_organizations__delta_table.alias('target').merge(
                source = enriched_organizations__df.alias('source')
                , condition = (F.col('source.HASH') == F.col('target.HASH'))
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

if __name__ == '__main__':
    main()