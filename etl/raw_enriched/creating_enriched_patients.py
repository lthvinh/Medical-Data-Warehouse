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

    raw_patients__file_name = 'patients.csv'
    enriched_patients__table_name = 'enriched_patients'
    raw_patients__full_path = f'{raw_path}/{raw_patients__file_name}'
    enriched_patients__full_path = f'{enriched_path}/{enriched_patients__table_name}'
    
    raw_patients__schema = T.StructType([
        T.StructField('ID', T.StringType(), True)
        , T.StructField('BIRTHDATE', T.DateType(), True)
        , T.StructField('DEATHDATE',T. DateType(), True)
        , T.StructField('SSN', T.StringType(), True)
        , T.StructField('DRIVERS', T.StringType(), True)
        , T.StructField('PASSPORT', T.StringType(), True)
        , T.StructField('PREFIX', T.StringType(), True)
        , T.StructField('FIRST', T.StringType(), True)
        , T.StructField('MIDDLE', T.StringType(), True)
        , T.StructField('LAST', T.StringType(), True)
        , T.StructField('SUFFIX', T.StringType(), True)
        , T.StructField('MAIDEN', T.StringType(), True)
        , T.StructField('MARITAL', T.StringType(), True)
        , T.StructField('RACE', T.StringType(), True)
        , T.StructField('ETHNICITY', T.StringType(), True)
        , T.StructField('GENDER', T.StringType(), True)
        , T.StructField('BIRTHPLACE', T.StringType(), True)
        , T.StructField('ADDRESS', T.StringType(), True)
        , T.StructField('CITY', T.StringType(), True)
        , T.StructField('STATE', T.StringType(), True)
        , T.StructField('COUNTY', T.StringType(), True)
        , T.StructField('FIPS', T.LongType(), True)
        , T.StructField('ZIP', T.LongType(), True)
        , T.StructField('LAT', T.DoubleType(), True)
        , T.StructField('LON', T.DoubleType(), True)
        , T.StructField('HEALTHCARE_EXPENSES', T.DoubleType(), True)
        , T.StructField('HEALTHCARE_COVERAGE', T.DoubleType(), True)
        , T.StructField('INCOME', T.LongType(), True)
    ])
    
    raw_patients__df = (
        spark.read.format('csv')
        .options(**{
            'path': raw_patients__full_path
            , 'header': True
            , 'inferSchema': False
        })
        .schema(raw_patients__schema)
        .load()
        .dropDuplicates()
    )
    
    enriched_patients__df = raw_patients__df.withColumn(
        'HASH', F.concat_ws('|', *[F.col(column_name) for column_name in raw_patients__df.columns])
    )
    
    enriched_patients__is_existing = DeltaTable.isDeltaTable(spark, enriched_patients__full_path)
    if not enriched_patients__is_existing:
        enriched_patients__df.write.format('delta').mode('overwrite').option('path', enriched_patients__full_path).saveAsTable(enriched_patients__table_name)
    else:
        enriched_patients__delta_table = DeltaTable.forName(spark, enriched_patients__table_name)
        (
            enriched_patients__delta_table.alias('target').merge(
                source = enriched_patients__df.alias('source')
                , condition = (F.col('source.HASH') == F.col('target.HASH'))
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

if __name__ == '__main__':
    main()