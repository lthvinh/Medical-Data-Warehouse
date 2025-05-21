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

    dim_organizations__table_name = 'dim_organizations'
    dim_organizations__full_path = f'{curated_path}/{dim_organizations__table_name}'
    enriched_organizations__table_name = 'enriched_organizations'
    enriched_organizations__full_path = f'{enriched_path}/{enriched_organizations__table_name}'
    enriched_organiztionss__df = spark.read.format('delta').option('path', enriched_organizations__full_path).load()
    
    dim_default_organizations__df = spark.sql('''
        SELECT
            '0000000000000000000000000000000000000000000000000000000000000000' AS Organization_Key
            , '00000000-0000-0000-0000-000000000000' AS Organization_Id
            , 'Unknown' AS Organization_Name
            , 'Unknown' AS Organization_Address
            , 'Unknown' AS Organization_City
            , 'Unknown' AS Organization_State
            , 'Unknown' AS Organization_Zip
            , CAST(0.0 AS DOUBLE) AS Organization_Lat
            , CAST(0.0 AS DOUBLE) AS Organization_Lon
            , 'Unknown' AS Organization_Phone
            , CAST(0.0 AS DOUBLE) AS Organization_Revenue
            , CAST(0 AS LONG) AS Organization_Utilization
            , CAST("1900-01-01 00:00:00" AS TIMESTAMP) AS Effective_Date
            , CAST('9999-12-31 00:00:00' AS TIMESTAMP) AS Expiration_Date
            , True AS Is_Current
    ''')
    
    dim_organizations__df = (
        enriched_organiztionss__df
        .select(
            F.ifnull(F.col('ID'), F.lit('00000000-0000-0000-0000-000000000000')).alias('Organization_Id')
            , F.ifnull(F.col('NAME'), F.lit('Unknown')).alias('Organization_Name')
            , F.ifnull(F.col('ADDRESS'), F.lit('Unknown')).alias('Organization_Address')
            , F.ifnull(F.col('CITY'), F.lit('Unknown')).alias('Organization_City')
            , F.ifnull(F.col('STATE'), F.lit('Unknown')).alias('Organization_State')
            , F.ifnull(F.col('ZIP'), F.lit('Unknown')).alias('Organization_Zip')
            , F.ifnull(F.col('LAT'), F.lit(0.0)).alias('Organization_Lat')
            , F.ifnull(F.col('LON'), F.lit(0.0)).alias('Organization_Lon')
            , F.ifnull(F.col('PHONE'), F.lit('Unknown')).alias('Organization_Phone')
            , F.ifnull(F.col('REVENUE'), F.lit(0.0)).alias('Organization_Revenue')
            , F.ifnull(F.col('UTILIZATION'), F.lit(0)).alias('Organization_Utilization')
        )
        .dropDuplicates()
        .select(
            F.sha2(
                F.concat_ws(
                    '|'
                    , F.col('Organization_Id')
                    , F.col('Organization_Name')
                    , F.col('Organization_Address')
                    , F.col('Organization_City')
                    , F.col('Organization_State')
                    , F.col('Organization_Zip')
                    , F.col('Organization_Lat')
                    , F.col('Organization_Lon')
                    , F.col('Organization_Phone')
                    , F.col('Organization_Revenue')
                    , F.col('Organization_Utilization')
                )
                , 256
            ).alias('Organization_Key')
            , F.col('Organization_Id')
            , F.col('Organization_Name')
            , F.col('Organization_Address')
            , F.col('Organization_City')
            , F.col('Organization_State')
            , F.col('Organization_Zip')
            , F.col('Organization_Lat')
            , F.col('Organization_Lon')
            , F.col('Organization_Phone')
            , F.col('Organization_Revenue')
            , F.col('Organization_Utilization')
            , F.current_timestamp().alias('Effective_Date') 
            , F.to_timestamp(F.lit('9999-12-31 00:00:00')).alias('Expiration_Date') 
            , F.lit(True).alias('Is_Current')
        )
        .unionAll(dim_default_organizations__df)
        .orderBy(F.col('Organization_Id').asc())
    )
    
    dim_organizations__is_existing = DeltaTable.isDeltaTable(spark, dim_organizations__full_path)
    if not dim_organizations__is_existing:
        dim_organizations__df.write.mode('overwrite').format('delta').option('path', dim_organizations__full_path).saveAsTable(dim_organizations__table_name)
    else:
        dim_organizations__delta_table = DeltaTable.forName(spark, dim_organizations__table_name)
        update_rows = (
            dim_organizations__df.alias('source')
            .join(
                dim_organizations__delta_table.toDF().where(F.col('Is_Current')).alias('target')
                , F.col('source.Organization_Id') == F.col('target.Organization_Id')
            )
            .where(F.col('source.Organization_Key') != F.col('target.Organization_Key'))
            .select(F.col('source.*'))
        )
        update_rows.write.mode('append').format('delta').option('path', dim_organizations__full_path).saveAsTable(dim_organizations__table_name)
        (
            dim_organizations__delta_table.alias('target').merge(
                source = dim_organizations__df.alias('source')
                , condition = (
                    (F.col('source.Organization_Id') == F.col('target.Organization_Id'))
                    & (F.col('target.Is_Current'))
                )
            )
            .whenMatchedUpdate(
                condition = (F.col('source.Organization_Key') != F.col('target.Organization_Key'))
                , set = {
                    'Is_Current': F.lit(False)
                    , 'Expiration_Date': F.current_timestamp()
                }
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

if __name__ == '__main__':
    main()