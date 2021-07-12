from airflow.models import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

dag = DAG(
    dag_id='datalake_dag',
    start_date=days_ago(2),
    schedule_interval='@once'
)

ingest_country_info_task = SparkSubmitOperator(
    application="/jars/spark-task-1.0-SNAPSHOT-all.jar",
    task_id="ingest_country_info_task",
    conn_id="spark_cluster",
    dag=dag,
    java_class="com.genestack.tasks.DeltaLakeFileIngestorKt",
    conf={"spark.standalone.submit.waitAppCompletion": "true"},
    application_args=[
        "s3a://genestack-spark-test/country-info.json",  # input file
        "json",  # input file format
        "country-info"  # output file
    ]
)

ingest_covid_deaths_task = SparkSubmitOperator(
    application="/jars/spark-task-1.0-SNAPSHOT-all.jar",
    task_id="ingest_covid_deaths_task",
    conn_id="spark_cluster",
    dag=dag,
    java_class="com.genestack.tasks.DeltaLakeFileIngestorKt",
    conf={"spark.standalone.submit.waitAppCompletion": "true"},
    application_args=[
        "s3a://genestack-spark-test/covid-deaths.csv",  # input file
        "csv",  # input file format
        "covid-deaths"  # output file
    ]
)

ingest_vaccinations_task = SparkSubmitOperator(
    application="/jars/spark-task-1.0-SNAPSHOT-all.jar",
    task_id="ingest_vaccinations_task",
    conn_id="spark_cluster",
    dag=dag,
    java_class="com.genestack.tasks.DeltaLakePostgresIngestorKt",
    conf={"spark.standalone.submit.waitAppCompletion": "true"},
    application_args=[
        "postgres-external-source",  # host
        "5432",  # port
        "external_data",  # db name
        "covid_vaccinations",  # table name
        "spark",  # username
        "dratuti",  # password (shh!)
        "covid_vaccinations"  # output file
    ]
)

covid_data_denormalization_task = SparkSubmitOperator(
    application="/jars/spark-task-1.0-SNAPSHOT-all.jar",
    task_id="covid_data_denormalization_task",
    conn_id="spark_cluster",
    dag=dag,
    java_class="com.genestack.tasks.CovidDataDenormalizationTaskKt",
    conf={"spark.standalone.submit.waitAppCompletion": "true"},
    trigger_rule=TriggerRule.ALL_DONE
)

covid_data_curation_task = SparkSubmitOperator(
    application="/jars/spark-task-1.0-SNAPSHOT-all.jar",
    task_id="covid_data_curation_task",
    conn_id="spark_cluster",
    dag=dag,
    java_class="com.genestack.tasks.CovidDataCurationTaskKt",
    conf={"spark.standalone.submit.waitAppCompletion": "true"},
    trigger_rule=TriggerRule.ALL_DONE
)

covid_curated_data_export_task = SparkSubmitOperator(
    application="/jars/spark-task-1.0-SNAPSHOT-all.jar",
    task_id="covid_curated_data_export_task",
    conn_id="spark_cluster",
    dag=dag,
    java_class="com.genestack.tasks.CovidDataMongoExportTaskKt",
    conf={"spark.standalone.submit.waitAppCompletion": "true"},
    trigger_rule=TriggerRule.ALL_DONE,
    application_args=[
        "mongo-external-source",  # host
        "27017",  # port
        "covid_data",  # db name
        "covid_data",  # collection name
        "mongo",  # username
        "dratuti"  # password (shh!)
    ]
)

ingest_country_info_task >> covid_data_denormalization_task
ingest_covid_deaths_task >> covid_data_denormalization_task
ingest_vaccinations_task >> covid_data_denormalization_task

covid_data_denormalization_task >> covid_data_curation_task >> covid_curated_data_export_task
