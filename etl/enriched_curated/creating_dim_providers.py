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

    dim_providers__table_name = 'dim_providers'
    dim_providers__full_path = f'{curated_path}/{dim_providers__table_name}'
    enriched_providers__table_name = 'enriched_providers'
    enriched_providers__full_path = f'{enriched_path}/{enriched_providers__table_name}'
    enriched_providers__df = spark.read.format('delta').option('path', enriched_providers__full_path).load()
    enriched_organizations__table_name = 'enriched_organizations'
    enriched_organizations__full_path = f'{enriched_path}/{enriched_organizations__table_name}'
    enriched_organiztionss__df = spark.read.format('delta').option('path', enriched_organizations__full_path).load()
    
    dim_default_providers__df = spark.sql('''
        SELECT
            '0000000000000000000000000000000000000000000000000000000000000000' AS Provider_Key
            , '00000000-0000-0000-0000-000000000000' AS Provider_Id
            , 'Unknown' AS Provider_Name
            , 'Unknown' AS Provider_Gender
            , 'Unknown' AS Provider_Speciality
            , 'Unknown' AS Provider_Address
            , 'Unknown' AS Provider_City
            , 'Unknown' AS Provider_State
            , 'Unknown' AS Provider_Zip
            , CAST(0.0 AS DOUBLE) AS Provider_Lat
            , CAST(0.0 AS DOUBLE) AS Provider_Lon
            , CAST(0 AS LONG) AS Provider_Utilization
            , 'Unknown' AS Organization_Name
            , 'Unknown' AS Organization_Address
            , 'Unknown' AS Organization_City
            , 'Unknown' AS Organization_State
            , 'Unknown' AS Organization_Zip
            , CAST(0.0 AS DOUBLE) AS Organization_Lat
            , CAST(0.0 AS DOUBLE) AS Organization_Lon
            , 'Unknown' AS Organization_Phone
            , CAST("1900-01-01 00:00:00" AS TIMESTAMP) AS Effective_Date
            , CAST('9999-12-31 00:00:00' AS TIMESTAMP) AS Expiration_Date
            , True AS Is_Current
    ''')
    dim_providers__df = (
        enriched_providers__df.alias('providers')
        .join(
            enriched_organiztionss__df.alias('organizations')
            , F.col('providers.ORGANIZATION') == F.col('organizations.ID')
            , 'left'
        )
        .select(
            F.ifnull(F.col('providers.ID'), F.lit('00000000-0000-0000-0000-000000000000')).alias('Provider_Id')
            , F.ifnull(F.col('providers.NAME'), F.lit('Unknown')).alias('Provider_Name')
            , F.ifnull(F.col('providers.GENDER'), F.lit('Unknown')).alias('Provider_Gender')
            , F.ifnull(F.col('providers.SPECIALITY'), F.lit('Unknown')).alias('Provider_Speciality')
            , F.ifnull(F.col('providers.ADDRESS'), F.lit('Unknown')).alias('Provider_Address')
            , F.ifnull(F.col('providers.CITY'), F.lit('Unknown')).alias('Provider_City')
            , F.ifnull(F.col('providers.STATE'), F.lit('Unknown')).alias('Provider_State')
            , F.ifnull(F.col('providers.ZIP'), F.lit('Unknown')).alias('Provider_Zip')
            , F.ifnull(F.col('providers.LAT'), F.lit(0.0)).alias('Provider_Lat')
            , F.ifnull(F.col('providers.LON'), F.lit(0.0)).alias('Provider_Lon')
            , F.ifnull(F.col('providers.ENCOUNTERS'), F.lit(0)).alias('Provider_Utilization')
            , F.ifnull(F.col('organizations.NAME'), F.lit('Unknown')).alias('Organization_Name')
            , F.ifnull(F.col('organizations.ADDRESS'), F.lit('Unknown')).alias('Organization_Address')
            , F.ifnull(F.col('organizations.CITY'), F.lit('Unknown')).alias('Organization_City')
            , F.ifnull(F.col('organizations.STATE'), F.lit('Unknown')).alias('Organization_State')
            , F.ifnull(F.col('organizations.ZIP'), F.lit('Unknown')).alias('Organization_Zip')
            , F.ifnull(F.col('organizations.LAT'), F.lit(0.0)).alias('Organization_Lat')
            , F.ifnull(F.col('organizations.LON'), F.lit(0.0)).alias('Organization_Lon')
            , F.ifnull(F.col('organizations.PHONE'), F.lit('Unknown')).alias('Organization_Phone')
        )
        .dropDuplicates()
        .select(
            F.sha2(
                F.concat_ws(
                    '|'
                    , F.col('Provider_Id')
                    , F.col('Provider_Name')
                    , F.col('Provider_Gender')
                    , F.col('Provider_Speciality')
                    , F.col('Provider_Address')
                    , F.col('Provider_City')
                    , F.col('Provider_State')
                    , F.col('Provider_Zip')
                    , F.col('Provider_Lat')
                    , F.col('Provider_Lon')
                    , F.col('Provider_Utilization')
                    , F.col('Organization_Name')
                    , F.col('Organization_Address')
                    , F.col('Organization_City')
                    , F.col('Organization_State')
                    , F.col('Organization_Zip')
                    , F.col('Organization_Lat')
                    , F.col('Organization_Lon')
                    , F.col('Organization_Phone')
                )
                , 256
            ).alias('Provider_Key')
            , F.col('Provider_Id')
            , F.col('Provider_Name')
            , F.col('Provider_Gender')
            , F.col('Provider_Speciality')
            , F.col('Provider_Address')
            , F.col('Provider_City')
            , F.col('Provider_State')
            , F.col('Provider_Zip')
            , F.col('Provider_Lat')
            , F.col('Provider_Lon')
            , F.col('Provider_Utilization')
            , F.col('Organization_Name')
            , F.col('Organization_Address')
            , F.col('Organization_City')
            , F.col('Organization_State')
            , F.col('Organization_Zip')
            , F.col('Organization_Lat')
            , F.col('Organization_Lon')
            , F.col('Organization_Phone')
            , F.current_timestamp().alias('Effective_Date') 
            , F.to_timestamp(F.lit('9999-12-31 00:00:00')).alias('Expiration_Date') 
            , F.lit(True).alias('Is_Current')
        )
        .unionAll(dim_default_providers__df)
    )
    
    dim_providers__is_existing = DeltaTable.isDeltaTable(spark, dim_providers__full_path)
    if not dim_providers__is_existing:
        dim_providers__df.write.mode('overwrite').format('delta').option('path', dim_providers__full_path).saveAsTable(dim_providers__table_name)
    else:
        dim_providers__delta_table = DeltaTable.forName(spark, dim_providers__table_name)
        update_rows = (
            dim_providers__df.alias('source')
            .join(
                dim_providers__delta_table.toDF().where(F.col('Is_Current')).alias('target')
                , F.col('source.Provider_Id') == F.col('target.Provider_Id')
            )
            .where(F.col('source.Provider_Key') != F.col('target.Provider_Key'))
            .select(F.col('source.*'))
        )
        update_rows.write.mode('append').format('delta').option('path', dim_providers__full_path).saveAsTable(dim_providers__table_name)
        (
            dim_providers__delta_table.alias('target').merge(
                source = dim_providers__df.alias('source')
                , condition = (
                    (F.col('source.Provider_Id') == F.col('target.Provider_Id'))
                    & (F.col('target.Is_Current'))
                )
            )
            .whenMatchedUpdate(
                condition = (F.col('source.Provider_Key') != F.col('target.Provider_Key'))
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