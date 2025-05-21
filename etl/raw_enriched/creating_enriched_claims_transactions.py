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

    raw_claims_transactions__file_name = 'claims_transactions.csv'
    enriched_claims_transactions__table_name = 'enriched_claims_transactions'
    raw_claims_transactions__full_path = f'{raw_path}/{raw_claims_transactions__file_name}'
    enriched_claims_transactions__full_path = f'{enriched_path}/{enriched_claims_transactions__table_name}'
    
    raw_claims_transactions__schema = T.StructType([
        T.StructField('ID', T.StringType(), True)
        , T.StructField('CLAIMID', T.StringType(), True)
        , T.StructField('CHARGEID', T.LongType(), True)
        , T.StructField('PATIENTID', T.StringType(), True)
        , T.StructField('TYPE', T.StringType(), True)
        , T.StructField('AMOUNT', T.DoubleType(), True)
        , T.StructField('METHOD', T.StringType(), True)
        , T.StructField('FROMDATE', T.TimestampType(), True)
        , T.StructField('TODATE', T.TimestampType(), True)
        , T.StructField('PLACEOFSERVICE', T.StringType(), True)
        , T.StructField('PROCEDURECODE', T.LongType(), True)
        , T.StructField('MODIFIER1', T.StringType(), True)
        , T.StructField('MODIFIER2', T.StringType(), True)
        , T.StructField('DIAGNOSISREF1', T.LongType(), True)
        , T.StructField('DIAGNOSISREF2', T.LongType(), True)
        , T.StructField('DIAGNOSISREF3', T.LongType(), True)
        , T.StructField('DIAGNOSISREF4', T.LongType(), True)
        , T.StructField('UNITS', T.LongType(), True)
        , T.StructField('DEPARTMENTID', T.LongType(), True)
        , T.StructField('NOTES', T.StringType(), True)
        , T.StructField('UNITAMOUNT', T.DoubleType(), True)
        , T.StructField('TRANSFEROUTID', T.LongType(), True)
        , T.StructField('TRANSFERTYPE', T.StringType(), True)
        , T.StructField('PAYMENTS', T.DoubleType(), True)
        , T.StructField('ADJUSTMENTS', T.DoubleType(), True)
        , T.StructField('TRANSFERS', T.DoubleType(), True)
        , T.StructField('OUTSTANDING', T.DoubleType(), True)
        , T.StructField('APPOINTMENTID', T.StringType(), True)
        , T.StructField('LINENOTE', T.StringType(), True)
        , T.StructField('PATIENTINSURANCEID', T.StringType(), True)
        , T.StructField('FEESCHEDULEID', T.LongType(), True)
        , T.StructField('PROVIDERID', T.StringType(), True)
        , T.StructField('SUPERVISINGPROVIDERID', T.StringType(), True)
    ])
    
    raw_claims_transactions__df = (
        spark.read.format('csv')
        .options(**{
            'path': raw_claims_transactions__full_path
            , 'header': True
            , 'inferSchema': False
        })
        .schema(raw_claims_transactions__schema)
        .load()
        .dropDuplicates()
    )
    
    enriched_claims_transactions__df = raw_claims_transactions__df.withColumn(
        'HASH', F.concat_ws('|', *[F.col(column_name) for column_name in raw_claims_transactions__df.columns])
    )
    
    enriched_claims_transactions__is_existing = DeltaTable.isDeltaTable(spark, enriched_claims_transactions__full_path)
    if not enriched_claims_transactions__is_existing:
        enriched_claims_transactions__df.write.format('delta').mode('overwrite').option('path', enriched_claims_transactions__full_path).saveAsTable(enriched_claims_transactions__table_name)
    else:
        enriched_claims_transactions__delta_table = DeltaTable.forName(spark, enriched_claims_transactions__table_name)
        (
            enriched_claims_transactions__delta_table.alias('target').merge(
                source = enriched_claims_transactions__df.alias('source')
                , condition = (F.col('source.HASH') == F.col('target.HASH'))
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
        
if __name__ == '__main__':
    main()