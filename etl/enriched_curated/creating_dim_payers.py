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
    
    dim_payers__table_name = 'dim_payers'
    dim_payers__full_path = f'{curated_path}/{dim_payers__table_name}'
    enriched_payers__table_name = 'enriched_payers'
    enriched_payers__full_path = f'{enriched_path}/{enriched_payers__table_name}'
    enriched_payers__df = spark.read.format('delta').option('path', enriched_payers__full_path).load()
    
    dim_default_payers__df = spark.sql('''
        SELECT
            '0000000000000000000000000000000000000000000000000000000000000000' AS Payer_Key
            , '00000000-0000-0000-0000-000000000000' AS Payer_Id
            , 'Unknown' AS Payer_Name
            , 'Unknown' AS Payer_Ownership
            , 'Unknown' AS Payer_Address
            , 'Unknown' AS Payer_City
            , 'Unknown' AS Payer_State_Headquartered
            , 'Unknown' AS Payer_Zip
            , 'Unknown' AS Payer_Phone
            , CAST(0.0 AS DOUBLE) AS Payer_Amount_Covered
            , CAST(0.0 AS DOUBLE) AS Payer_Amount_Uncovered
            , CAST(0.0 AS DOUBLE) AS Payer_Revenue
            , CAST(0 AS LONG) AS Payer_Covered_Encounters
            , CAST(0 AS LONG) AS Payer_Uncovered_Encounters
            , CAST(0 AS LONG) AS Payer_Covered_Medications
            , CAST(0 AS LONG) AS Payer_Uncovered_Medications
            , CAST(0 AS LONG) AS Payer_Covered_Procedures
            , CAST(0 AS LONG) AS Payer_Uncovered_Procedures
            , CAST(0 AS LONG) AS Payer_Covered_Immunizations
            , CAST(0 AS LONG) AS Payer_Uncovered_Immunizations
            , CAST(0 AS LONG) AS Payer_Unique_Customers
            , CAST(0.0 AS DOUBLE) AS Payer_QOLS_Avg
            , CAST(0 AS LONG) AS Payer_Member_Months
            , CAST("1900-01-01 00:00:00" AS TIMESTAMP) AS Effective_Date
            , CAST('9999-12-31 00:00:00' AS TIMESTAMP) AS Expiration_Date
            , True AS Is_Current
    ''')
    
    dim_payers__df = (
        enriched_payers__df
        .select(
            F.ifnull(F.col('ID'), F.lit('00000000-0000-0000-0000-000000000000')).alias('Payer_Id')
            , F.ifnull(F.col('NAME'), F.lit('Unknown')).alias('Payer_Name')
            , F.ifnull(F.col('OWNERSHIP'), F.lit('Unknown')).alias('Payer_Ownership')
            , F.ifnull(F.col('ADDRESS'), F.lit('Unknown')).alias('Payer_Address')
            , F.ifnull(F.col('CITY'), F.lit('Unknown')).alias('Payer_City')
            , F.ifnull(F.col('STATE_HEADQUARTERED'), F.lit('Unknown')).alias('Payer_State_Headquartered')
            , F.ifnull(F.col('ZIP'), F.lit('Unknown')).alias('Payer_Zip')
            , F.ifnull(F.col('PHONE'), F.lit('Unknown')).alias('Payer_Phone')
            , F.ifnull(F.col('AMOUNT_COVERED'), F.lit(0.0)).alias('Payer_Amount_Covered')
            , F.ifnull(F.col('AMOUNT_UNCOVERED'), F.lit(0.0)).alias('Payer_Amount_Uncovered')
            , F.ifnull(F.col('REVENUE'), F.lit(0.0)).alias('Payer_Revenue')
            , F.ifnull(F.col('COVERED_ENCOUNTERS'), F.lit(0)).alias('Payer_Covered_Encounters')
            , F.ifnull(F.col('UNCOVERED_ENCOUNTERS'), F.lit(0)).alias('Payer_Uncovered_Encounters')
            , F.ifnull(F.col('COVERED_MEDICATIONS'), F.lit(0)).alias('Payer_Covered_Medications')
            , F.ifnull(F.col('UNCOVERED_MEDICATIONS'), F.lit(0)).alias('Payer_Uncovered_Medications')
            , F.ifnull(F.col('COVERED_PROCEDURES'), F.lit(0)).alias('Payer_Covered_Procedures')
            , F.ifnull(F.col('UNCOVERED_PROCEDURES'), F.lit(0)).alias('Payer_Uncovered_Procedures')
            , F.ifnull(F.col('COVERED_IMMUNIZATIONS'), F.lit(0)).alias('Payer_Covered_Immunizations')
            , F.ifnull(F.col('UNCOVERED_IMMUNIZATIONS'), F.lit(0)).alias('Payer_Uncovered_Immunizations')
            , F.ifnull(F.col('UNIQUE_CUSTOMERS'), F.lit(0)).alias('Payer_Unique_Customers')
            , F.ifnull(F.col('QOLS_AVG'), F.lit(0.0)).alias('Payer_QOLS_Avg')
            , F.ifnull(F.col('MEMBER_MONTHS'), F.lit(0)).alias('Payer_Member_Months')
        )
        .dropDuplicates()
        .select(
            F.sha2(
                F.concat_ws(
                    '|'
                    , F.col('Payer_Id')
                    , F.col('Payer_Name')
                    , F.col('Payer_Ownership')
                    , F.col('Payer_Address')
                    , F.col('Payer_City')
                    , F.col('Payer_State_Headquartered')
                    , F.col('Payer_Zip')
                    , F.col('Payer_Phone')
                    , F.col('Payer_Amount_Covered')
                    , F.col('Payer_Amount_Uncovered')
                    , F.col('Payer_Revenue')
                    , F.col('Payer_Covered_Encounters')
                    , F.col('Payer_Uncovered_Encounters')
                    , F.col('Payer_Covered_Medications')
                    , F.col('Payer_Uncovered_Medications')
                    , F.col('Payer_Covered_Procedures')
                    , F.col('Payer_Uncovered_Procedures')
                    , F.col('Payer_Covered_Immunizations')
                    , F.col('Payer_Uncovered_Immunizations')
                    , F.col('Payer_Unique_Customers')
                    , F.col('Payer_QOLS_Avg')
                    , F.col('Payer_Member_Months')
                )
                ,256
            ).alias('Payer_Key')
            , F.col('Payer_Id')
            , F.col('Payer_Name')
            , F.col('Payer_Ownership')
            , F.col('Payer_Address')
            , F.col('Payer_City')
            , F.col('Payer_State_Headquartered')
            , F.col('Payer_Zip')
            , F.col('Payer_Phone')
            , F.col('Payer_Amount_Covered')
            , F.col('Payer_Amount_Uncovered')
            , F.col('Payer_Revenue')
            , F.col('Payer_Covered_Encounters')
            , F.col('Payer_Uncovered_Encounters')
            , F.col('Payer_Covered_Medications')
            , F.col('Payer_Uncovered_Medications')
            , F.col('Payer_Covered_Procedures')
            , F.col('Payer_Uncovered_Procedures')
            , F.col('Payer_Covered_Immunizations')
            , F.col('Payer_Uncovered_Immunizations')
            , F.col('Payer_Unique_Customers')
            , F.col('Payer_QOLS_Avg')
            , F.col('Payer_Member_Months')
            , F.current_timestamp().alias('Effective_Date') 
            , F.to_timestamp(F.lit('9999-12-31 00:00:00')).alias('Expiration_Date') 
            , F.lit(True).alias('Is_Current')
        )
        .unionAll(dim_default_payers__df)
    )
    
    dim_payers__is_existing = DeltaTable.isDeltaTable(spark, dim_payers__full_path)
    if not dim_payers__is_existing:
        dim_payers__df.write.mode('overwrite').format('delta').option('path', dim_payers__full_path).saveAsTable(dim_payers__table_name)
    else:
        dim_payers__delta_table = DeltaTable.forName(spark, dim_payers__table_name)
        update_rows = (
            dim_payers__df.alias('source')
            .join(
                dim_payers__delta_table.toDF().where(F.col('Is_Current')).alias('target')
                , F.col('source.Payer_Id') == F.col('target.Payer_Id')
            )
            .where(F.col('source.Payer_Key') != F.col('target.Payer_Key'))
            .select(F.col('source.*'))
        )
        update_rows.write.mode('append').format('delta').option('path', dim_payers__full_path).saveAsTable(dim_payers__table_name)
        (
            dim_payers__delta_table.alias('target').merge(
                source = dim_payers__df.alias('source')
                , condition = (
                    (F.col('source.Payer_Id') == F.col('target.Payer_Id'))
                    & (F.col('target.Is_Current'))
                )
            )
            .whenMatchedUpdate(
                condition = (F.col('source.Payer_Key') != F.col('target.Payer_Key'))
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