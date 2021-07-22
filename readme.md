### DataLake PoC project

#### Run Spark-Hadoop cluster:

Note: If you don't need s3, you can go to #4

1) Ask DevOps to give you `Access key ID` and `Secret access key` to access to `genestack-spark-test` bucket
2) Add to your profile (.zprofile if you use zsh): following environment variables:

`export AWS_ACCESS_KEY_ID=your_access_key_id`
   
`export AWS_SECRET_ACCESS_KEY=your_secret_access_key`

3) Then execute `source ~/.zprofile`
4) Go to project root directory: `cd your_workspace_path/DataLakePoC`   
5) Build docker images: `cluster/build-docker.sh`
6) Run cluster: `cluster/start-cluster.sh`

Spark cluster master UI will be available on `http://localhost:8080`

Worker-1 UI: `http://localhost:8081` 

Worker-2 UI: `http://localhost:8082`

HDFS UI: `http://localhost:9870/dfshealth.html#tab-overview`

#### Run spark jobs using Spark-Hadoop spark shell:
   
Connect to cluster: `cluster/spark-shell-cluster.sh`

Execute: `spark.read.json("s3a://genestack-spark-test/country-info.json").show`

Spark cluster will read json file from aws s3 and show its contents in console

See more examples in `com.genestack.DeltaLakeTest`

To exit press `Ctrl+C`

#### Stop Spark-Hadoop cluster (all data will be removed):

`cluster/stop-cluster.sh`

#### Run datalake pipeline using airflow:

1) Build airflow image: `airflow/build-docker-airflow.sh
2) Run airflow cluster: `cd airflow; docker-compose up -d`
3) Run external sources cluster: `cd external-sources; docker-compose up -d`. 
   This will launch postgres and mongo databases needed to execute pipeline
4) Go to http://localhost:8079 Connections tab and create new connection:
   - Name: spark_cluster
   - Type: spark
   - Master url: spark://master
   - Port: 7077
   - Additional config: {"deploy-mode": "cluster"}
   
5) Go to `http://localhost:8079/graph?dag_id=datalake_dag` and enable `datalake_dag` by switching toggle

DAG should automatically start to execute. 
Due to some bug, tasks finish in fail state despite completing successfully.
