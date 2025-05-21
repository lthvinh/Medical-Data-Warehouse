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

    raw_payers__file_name = 'payers.csv'
    enriched_payers__table_name = 'enriched_payers'
    raw_payers__full_path = f'{raw_path}/{raw_payers__file_name}'
    enriched_payers__full_path = f'{enriched_path}/{enriched_payers__table_name}'
    
    raw_payers__schema = T.StructType([
        T.StructField('ID', T.StringType(), True)
        , T.StructField('NAME', T.StringType(), True)
        , T.StructField('OWNERSHIP', T.StringType(), True)
        , T.StructField('ADDRESS', T.StringType(), True)
        , T.StructField('CITY', T.StringType(), True)
        , T.StructField('STATE_HEADQUARTERED', T.StringType(), True)
        , T.StructField('ZIP', T.StringType(), True)
        , T.StructField('PHONE', T.StringType(), True)
        , T.StructField('AMOUNT_COVERED', T.DoubleType(), True)
        , T.StructField('AMOUNT_UNCOVERED', T.DoubleType(), True)
        , T.StructField('REVENUE', T.DoubleType(), True)
        , T.StructField('COVERED_ENCOUNTERS', T.LongType(), True)
        , T.StructField('UNCOVERED_ENCOUNTERS', T.LongType(), True)
        , T.StructField('COVERED_MEDICATIONS', T.LongType(), True)
        , T.StructField('UNCOVERED_MEDICATIONS', T.LongType(), True)
        , T.StructField('COVERED_PROCEDURES', T.LongType(), True)
        , T.StructField('UNCOVERED_PROCEDURES', T.LongType(), True)
        , T.StructField('COVERED_IMMUNIZATIONS', T.LongType(), True)
        , T.StructField('UNCOVERED_IMMUNIZATIONS', T.LongType(), True)
        , T.StructField('UNIQUE_CUSTOMERS', T.LongType(), True)
        , T.StructField('QOLS_AVG', T.DoubleType(), True)
        , T.StructField('MEMBER_MONTHS', T.LongType(), True)
    ])
    
    raw_payers__df = (
        spark.read.format('csv')
        .options(**{
            'path': raw_payers__full_path
            , 'header': True
            , 'inferSchema': False
        })
        .schema(raw_payers__schema)
        .load()
        .dropDuplicates()
    )
    
    enriched_payers__df = raw_payers__df.withColumn(
        'HASH', F.concat_ws('|', *[F.col(column_name) for column_name in raw_payers__df.columns])
    )
    
    enriched_payers__is_existing = DeltaTable.isDeltaTable(spark, enriched_payers__full_path)
    if not enriched_payers__is_existing:
        enriched_payers__df.write.format('delta').mode('overwrite').option('path', enriched_payers__full_path).saveAsTable(enriched_payers__table_name)
    else:
        enriched_payers__delta_table = DeltaTable.forName(spark, enriched_payers__table_name)
        (
            enriched_payers__delta_table.alias('target').merge(
                source = enriched_payers__df.alias('source')
                , condition = (F.col('source.HASH') == F.col('target.HASH'))
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

if __name__ == '__main__':
    main()