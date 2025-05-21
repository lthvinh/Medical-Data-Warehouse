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
    
    dim_patients__table_name = 'dim_patients'
    enriched_patients__table_name = 'enriched_patients'
    enriched_patients__full_path = f'{enriched_path}/{enriched_patients__table_name}'
    enriched_patients__df = spark.read.format('delta').option('path', enriched_patients__full_path).load()
    dim_patients__full_path = f'{curated_path}/{dim_patients__table_name}'
    
    dim_default_patients__df = spark.sql('''
        SELECT
            '0000000000000000000000000000000000000000000000000000000000000000' AS Patient_Key
            , '00000000-0000-0000-0000-000000000000' AS Patient_Id
            , TO_DATE("1900-01-01") AS Patient_Birth_Date
            , TO_DATE("9999-12-31") AS Patient_Death_Date
            , "Unknown" AS Patient_SSN
            , "Unknown" AS Patient_Drivers
            , "Unknown"  AS Patient_Passport
            , ""  AS Patient_Prefix
            , "Unknown" AS Patient_First
            , ""  AS Patient_Middle
            , ""  AS Patient_Last
            , "" AS Patient_Suffix
            , ""  AS Patient_Maiden
            , "Unknown" AS Patient_Marital
            , "Unknown" AS Patient_Race
            , "Unknown" AS Patient_Ethnicity
            , "Unknown" AS Patient_Gender
            , "Unknown" AS Patient_BirthPlace
            , "Unknown" AS Patient_Address
            , "Unknown" AS Patient_City
            , "Unknown" AS Patient_State
            , "Unknown" AS Patient_County
            , "Unknown" AS Patient_FIPS_County_Code
            , "Unknown" AS Patient_Zip
            , CAST(0.0 AS DOUBLE) AS Patient_Lat
            , CAST(0.0 AS DOUBLE) AS Patient_Lon
            , CAST(0.0 AS DOUBLE) AS Patient_Healthcare_Expenses
            , CAST(0.0 AS DOUBLE) AS Patient_Healthcare_Coverage
            , CAST(0.0 AS DOUBLE) AS Patient_Income
            , CAST("1900-01-01 00:00:00" AS TIMESTAMP) AS Effective_Date
            , CAST("9999-12-31 00:00:00" AS TIMESTAMP) AS Expiration_Date
            , True AS Is_Current
    ''')
    
    dim_patients__df = (
        enriched_patients__df
        .select(
            F.ifnull(F.col('ID'), F.lit('00000000-0000-0000-0000-000000000000')).alias('Patient_Id')
            , F.ifnull(F.col('BIRTHDATE'), F.to_date(F.lit('1900-01-01'))).alias('Patient_Birth_Date')
            , F.ifnull(F.col('DEATHDATE'), F.to_date(F.lit('9999-12-31'))).alias('Patient_Death_Date')
            , F.ifnull(F.col('SSN'), F.lit('Unknown')).alias('Patient_SSN')
            , F.ifnull(F.col('DRIVERS'), F.lit('Unknown')).alias('Patient_Drivers')
            , F.ifnull(F.col('PASSPORT'), F.lit('Unknown')).alias('Patient_Passport')
            , F.ifnull(F.col('PREFIX'), F.lit('')).alias('Patient_Prefix')
            , F.ifnull(F.col('FIRST'), F.lit('Unknown')).alias('Patient_First')
            , F.ifnull(F.col('MIDDLE'), F.lit('')).alias('Patient_Middle')
            , F.ifnull(F.col('LAST'), F.lit('')).alias('Patient_Last')
            , F.ifnull(F.col('SUFFIX'), F.lit('')).alias('Patient_Suffix')
            , F.ifnull(F.col('MAIDEN'), F.lit('')).alias('Patient_Maiden')
            , F.ifnull(F.col('MARITAL'), F.lit('Unknown')).alias('Patient_Marital')
            , F.ifnull(F.col('RACE'), F.lit('Unknown')).alias('Patient_Race')
            , F.ifnull(F.col('ETHNICITY'), F.lit('Unknown')).alias('Patient_Ethnicity')
            , F.ifnull(F.col('GENDER'), F.lit('Unknown')).alias('Patient_Gender')
            , F.ifnull(F.col('BIRTHPLACE'), F.lit('Unknown')).alias('Patient_BirthPlace')
            , F.ifnull(F.col('ADDRESS'), F.lit('Unknown')).alias('Patient_Address')
            , F.ifnull(F.col('CITY'), F.lit('Unknown')).alias('Patient_City')
            , F.ifnull(F.col('STATE'), F.lit('Unknown')).alias('Patient_State')
            , F.ifnull(F.col('COUNTY'), F.lit('Unknown')).alias('Patient_County')
            , F.ifnull(F.col('FIPS'), F.lit('Unknown')).alias('Patient_FIPS_County_Code')
            , F.ifnull(F.col('ZIP'), F.lit('Unknown')).alias('Patient_Zip')
            , F.ifnull(F.col('LAT'), F.lit(0.0)).alias('Patient_Lat')
            , F.ifnull(F.col('LON'), F.lit(0.0)).alias('Patient_Lon')
            , F.ifnull(F.col('HEALTHCARE_EXPENSES'), F.lit(0.0)).alias('Patient_Healthcare_Expenses')
            , F.ifnull(F.col('HEALTHCARE_COVERAGE'), F.lit(0.0)).alias('Patient_Healthcare_Coverage')
            , F.ifnull(F.col('INCOME'), F.lit(0.0)).alias('Patient_Income')
        )
        .dropDuplicates()
        .select(
            F.sha2(
                F.concat_ws(
                    '|'
                    , F.col('Patient_Id')
                    , F.col('Patient_Birth_Date')
                    , F.col('Patient_Death_Date')
                    , F.col('Patient_SSN')
                    , F.col('Patient_Drivers')
                    , F.col('Patient_Passport')
                    , F.col('Patient_Prefix')
                    , F.col('Patient_First')
                    , F.col('Patient_Middle')
                    , F.col('Patient_Last')
                    , F.col('Patient_Suffix')
                    , F.col('Patient_Maiden')
                    , F.col('Patient_Marital')
                    , F.col('Patient_Race')
                    , F.col('Patient_Ethnicity')
                    , F.col('Patient_Gender')
                    , F.col('Patient_BirthPlace')
                    , F.col('Patient_Address')
                    , F.col('Patient_City')
                    , F.col('Patient_State')
                    , F.col('Patient_County')
                    , F.col('Patient_FIPS_County_Code')
                    , F.col('Patient_Zip')
                    , F.col('Patient_Lat')
                    , F.col('Patient_Lon')
                    , F.col('Patient_Healthcare_Expenses')
                    , F.col('Patient_Healthcare_Coverage')
                    , F.col('Patient_Income')
                )
                , 256
            ).alias('Patient_Key')
            , F.col('Patient_Id')
            , F.col('Patient_Birth_Date')
            , F.col('Patient_Death_Date')
            , F.col('Patient_SSN')
            , F.col('Patient_Drivers')
            , F.col('Patient_Passport')
            , F.col('Patient_Prefix')
            , F.col('Patient_First')
            , F.col('Patient_Middle')
            , F.col('Patient_Last')
            , F.col('Patient_Suffix')
            , F.col('Patient_Maiden')
            , F.col('Patient_Marital')
            , F.col('Patient_Race')
            , F.col('Patient_Ethnicity')
            , F.col('Patient_Gender')
            , F.col('Patient_BirthPlace')
            , F.col('Patient_Address')
            , F.col('Patient_City')
            , F.col('Patient_State')
            , F.col('Patient_County')
            , F.col('Patient_FIPS_County_Code')
            , F.col('Patient_Zip')
            , F.col('Patient_Lat')
            , F.col('Patient_Lon')
            , F.col('Patient_Healthcare_Expenses')
            , F.col('Patient_Healthcare_Coverage')
            , F.col('Patient_Income')
            , F.current_timestamp().alias('Effective_Date') 
            , F.to_timestamp(F.lit('9999-12-31 00:00:00')).alias('Expiration_Date') 
            , F.lit(True).alias('Is_Current')
        )
        .unionAll(dim_default_patients__df)
        .orderBy(F.col('Patient_Id').asc())
    )
    dim_patients__is_existing = DeltaTable.isDeltaTable(spark, dim_patients__full_path)
    if not dim_patients__is_existing:
        dim_patients__df.write.mode('overwrite').format('delta').option('path', dim_patients__full_path).saveAsTable(dim_patients__table_name)
    else:
        dim_patients__delta_table = DeltaTable.forName(spark, dim_patients__table_name)
        update_rows = (
            dim_patients__df.alias('source')
            .join(
                dim_patients__delta_table.toDF().where(F.col('Is_Current')).alias('target')
                , F.col('source.Patient_Id') == F.col('target.Patient_Id')
            )
            .where(F.col('source.Patient_Key') != F.col('target.Patient_Key'))
            .select(F.col('source.*'))
        )
        update_rows.write.mode('append').format('delta').option('path', dim_patients__full_path).saveAsTable(dim_patients__table_name)
        (
            dim_patients__delta_table.alias('target').merge(
                source = dim_patients__df.alias('source')
                , condition = (
                    (F.col('source.Patient_Id') == F.col('target.Patient_Id'))
                    & (F.col('target.Is_Current'))
                )
            )
            .whenMatchedUpdate(
                condition = (F.col('source.Patient_Key') != F.col('target.Patient_Key'))
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