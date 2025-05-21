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

    dim_date__table_name = 'dim_date'
    dim_date__full_path = f'{curated_path}/{dim_date__table_name}'
    dim_date__is_existing = DeltaTable.isDeltaTable(spark, dim_date__full_path)
    
    if not dim_date__is_existing:
        start_date = '1990-01-01'
        end_date = '2023-01-01'
        
        creating_dim_date__sql = f"""
            SELECT
                explode(sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)) as Date
            UNION ALL
            SELECT to_date('9999-12-31')
        """
        date__df = spark.sql(creating_dim_date__sql)
        dim_date__df = date__df.select(
            F.date_format(F.col('Date'), 'yyyyMMdd').cast(T.IntegerType()).alias('Date_Key')
            , F.col('Date')
            , F.year(F.col('Date')).alias('Year')
            , F.month(F.col('Date')).alias('Month_Number')
            , F.date_format(F.col('Date'), 'MMM').alias('Month_Name_Short')
            , F.date_format(F.col('Date'), 'MMMM').alias('Month_Name_Long')
            , F.dayofweek(F.col('Date')).alias('Day_Of_Week_Number')
            , F.date_format(F.col('Date'), 'EEEE').alias('Day_Of_Week')
            , F.date_format(F.col('Date'), 'E').alias('Day_Of_Week_Short')
            , F.quarter(F.col('Date')).alias('quarter')
            , F.concat_ws('_Q', F.year(F.col('Date')), F.quarter(F.col('Date'))).alias('Year_Quarter')
            , F.last_day(F.col('Date')).alias('End_Of_Month')
        )
        dim_date__df.write.mode('overwrite').format('delta').option('path', dim_date__full_path).saveAsTable(dim_date__table_name)

if __name__ == '__main__':
    main()