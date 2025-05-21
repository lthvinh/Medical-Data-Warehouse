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

    raw_claims__file_name = 'claims.csv'
    enriched_claims__table_name = 'enriched_claims'
    raw_claims__full_path = f'{raw_path}/{raw_claims__file_name}'
    enriched_claims__full_path = f'{enriched_path}/{enriched_claims__table_name}'
    
    raw_claims__schema = T.StructType([
        T.StructField('ID', T.StringType(), True)
        , T.StructField('PATIENTID', T.StringType(), True)
        , T.StructField('PROVIDERID', T.StringType(), True)
        , T.StructField('PRIMARYPATIENTINSURANCEID', T.StringType(), True)
        , T.StructField('SECONDARYPATIENTINSURANCEID', T.StringType(), True)
        , T.StructField('DEPARTMENTID', T.LongType(), True)
        , T.StructField('PATIENTDEPARTMENTID', T.LongType(), True)
        , T.StructField('DIAGNOSIS1', T.LongType(), True)
        , T.StructField('DIAGNOSIS2', T.LongType(), True)
        , T.StructField('DIAGNOSIS3', T.LongType(), True)
        , T.StructField('DIAGNOSIS4', T.LongType(), True)
        , T.StructField('DIAGNOSIS5', T.LongType(), True)
        , T.StructField('DIAGNOSIS6', T.LongType(), True)
        , T.StructField('DIAGNOSIS7', T.LongType(), True)
        , T.StructField('DIAGNOSIS8', T.LongType(), True)
        , T.StructField('REFERRINGPROVIDERID', T.StringType(), True)
        , T.StructField('APPOINTMENTID', T.StringType(), True)
        , T.StructField('CURRENTILLNESSDATE', T.TimestampType(), True)
        , T.StructField('SERVICEDATE', T.TimestampType(), True)
        , T.StructField('SUPERVISINGPROVIDERID', T.StringType(), True)
        , T.StructField('STATUS1', T.StringType(), True)
        , T.StructField('STATUS2', T.StringType(), True)
        , T.StructField('STATUSP', T.StringType(), True)
        , T.StructField('OUTSTANDING1', T.DoubleType(), True)
        , T.StructField('OUTSTANDING2', T.DoubleType(), True)
        , T.StructField('OUTSTANDINGP', T.DoubleType(), True)
        , T.StructField('LASTBILLEDDATE1', T.TimestampType(), True)
        , T.StructField('LASTBILLEDDATE2', T.TimestampType(), True)
        , T.StructField('LASTBILLEDDATEP', T.TimestampType(), True)
        , T.StructField('HEALTHCARECLAIMTYPEID1', T.LongType(), True)
        , T.StructField('HEALTHCARECLAIMTYPEID2', T.LongType(), True)
    ])

    raw_claims__df = (
        spark.read.format('csv')
        .options(**{
            'path': raw_claims__full_path
            , 'header': True
            , 'inferSchema': False
        })
        .schema(raw_claims__schema)
        .load()
        .dropDuplicates()
    )
    
    enriched_claims__df = raw_claims__df.withColumn(
        'HASH', F.concat_ws('|', *[F.col(column_name) for column_name in raw_claims__df.columns])
    )
    
    enriched_claims__is_existing = DeltaTable.isDeltaTable(spark, enriched_claims__full_path)
    if not enriched_claims__is_existing:
        enriched_claims__df.write.format('delta').mode('overwrite').option('path', enriched_claims__full_path).saveAsTable(enriched_claims__table_name)
    else:
        enriched_claims__delta_table = DeltaTable.forName(spark, enriched_claims__table_name)
        (
            enriched_claims__delta_table.alias('target').merge(
                source = enriched_claims__df.alias('source')
                , condition = (F.col('source.HASH') == F.col('target.HASH'))
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

if __name__ == '__main__':
    main()