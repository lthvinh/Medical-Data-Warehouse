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

    raw_payer_transitions__file_name = 'payer_transitions.csv'
    enriched_payer_transitions__table_name = 'enriched_payer_transitions'
    raw_payer_transitions__full_path = f'{raw_path}/{raw_payer_transitions__file_name}'
    enriched_payer_transitions__full_path = f'{enriched_path}/{enriched_payer_transitions__table_name}'
    
    raw_payer_transitions__schema = T.StructType([
        T.StructField('PATIENT', T.StringType(), True)
        , T.StructField('MEMBERID', T.StringType(), True)
        , T.StructField('START_DATE', T.TimestampType(), True)
        , T.StructField('END_DATE', T.TimestampType(), True)
        , T.StructField('PAYER', T.StringType(), True)
        , T.StructField('SECONDARY_PAYER', T.StringType(), True)
        , T.StructField('PLAN_OWNERSHIP', T.StringType(), True)
        , T.StructField('OWNER_NAME', T.StringType(), True)
    ])
    
    raw_payer_transitions__df = (
        spark.read.format('csv')
        .options(**{
            'path': raw_payer_transitions__full_path
            , 'header': True
            , 'inferSchema': False
        })
        .schema(raw_payer_transitions__schema)
        .load()
        .dropDuplicates()
    )
    
    enriched_payer_transitions__df = raw_payer_transitions__df.withColumn(
        'HASH', F.concat_ws('|', *[F.col(column_name) for column_name in raw_payer_transitions__df.columns])
    )
    
    enriched_payer_transitions__is_existing = DeltaTable.isDeltaTable(spark, enriched_payer_transitions__full_path)
    if not enriched_payer_transitions__is_existing:
        enriched_payer_transitions__df.write.format('delta').mode('overwrite').option('path', enriched_payer_transitions__full_path).saveAsTable(enriched_payer_transitions__table_name)
    else:
        enriched_payer_transitions__delta_table = DeltaTable.forName(spark, enriched_payer_transitions__table_name)
        (
            enriched_payer_transitions__delta_table.alias('target').merge(
                source = enriched_payer_transitions__df.alias('source')
                , condition = (F.col('source.HASH') == F.col('target.HASH'))
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
        
if __name__ == '__main__':
    main()