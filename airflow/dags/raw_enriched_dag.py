from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator
import pendulum

default_args = {
    'owner': 'vinh'
    , 'email': 'ltvinh1101@gmail.com'
    , 'email_on_failure': True
    , 'email_on_retry': True
    , 'retries': 1
    , 'retry_delay': pendulum.duration(seconds = 1)
}

raw_enriched_apps_path = '/opt/etl/raw_enriched'
enriched_curated_apps_path = '/opt/etl/enriched_curated'
conn_id = 'spark_conn'
packages = 'io.delta:delta-spark_2.12:3.3.0,org.apache.hadoop:hadoop-aws:3.3.4'

with DAG(
    dag_id = 'medical_dag_v17'
    , description = 'this is my first dag'
    , start_date = pendulum.now(tz = 'Asia/Ho_Chi_Minh')
    , max_active_tasks = 1
    , max_active_runs = 1
    , schedule = None
) as dag:
    # start = EmptyOperator(task_id = 'start')

    # with TaskGroup(group_id = 'raw_enriched_group') as raw_enriched_group:
    #     raw_enriched_apps = [
    #         'creating_enriched_allergies'
    #         , 'creating_enriched_care_plans'
    #         , 'creating_enriched_claims_transactions'
    #         , 'creating_enriched_claims'
    #         , 'creating_enriched_conditions'
    #         , 'creating_enriched_devices'
    #         , 'creating_enriched_encounters'
    #         , 'creating_enriched_imaging_studies'
    #         , 'creating_enriched_immunizations'
    #         , 'creating_enriched_medications'
    #         , 'creating_enriched_observations'
    #         , 'creating_enriched_organizations'
    #         , 'creating_enriched_patients'
    #         , 'creating_enriched_payer_transitions'
    #         , 'creating_enriched_payers'
    #         , 'creating_enriched_procedures'
    #         , 'creating_enriched_providers'
    #         , 'creating_enriched_supplies'
    #     ]
    #     raw_enriched_operators = []
    #     for app in raw_enriched_apps:
    #         operator = SparkSubmitOperator(
    #             task_id = app
    #             , conn_id = conn_id
    #             , application = f'{raw_enriched_apps_path}/{app}.py'
    #             , packages = packages
    #         )
    #         raw_enriched_operators.append(operator)

    #     start >> raw_enriched_group
    
    with TaskGroup(group_id = 'dim_group') as dim_group:
        dim_group = [
            'creating_dim_allergies'
            , 'creating_dim_care_plans'
            , 'creating_dim_conditions'
            , 'creating_dim_date'
            , 'creating_dim_devices'
            , 'creating_dim_encounter_codes'
            , 'creating_dim_encounter_reason_codes'
            , 'creating_dim_imaging_studies'
            , 'creating_dim_immunizations'
            , 'creating_dim_medications'
            , 'creating_dim_observations'
            , 'creating_dim_organizations'
            , 'creating_dim_patients'
            , 'creating_dim_payers'
            , 'creating_dim_procedures'
            , 'creating_dim_providers'
            , 'creating_dim_supplies'
            , 'creating_dim_time'
        ]

        dim_group_operators = []
        for app in dim_group:
            operator = SparkSubmitOperator(
                task_id = app
                , conn_id = conn_id
                , application = f'{enriched_curated_apps_path}/{app}.py'
                , packages = packages
            )
            dim_group_operators.append(operator)

    # raw_enriched_group >>  dim_group_operators

    creating_fact_encounters_app = 'creating_fact_encounters'

    creating_fact_encounters_operator = SparkSubmitOperator(
        task_id = creating_fact_encounters_app
        , conn_id = conn_id
        , application = f'{enriched_curated_apps_path}/{creating_fact_encounters_app}.py'
        , packages = packages
    )
    dim_group_operators >> creating_fact_encounters_operator