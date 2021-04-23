### DataLake PoC project

#### Run local Spark-Hadoop cluster:

Note: If you don't need s3, you can go to #4

1) Ask DevOps to give you `Access key ID` and `Secret access key` to access to `genestack-spark-test` bucket
2) Add to your profile (.zprofile if you use zsh): following environment variables:

`export AWS_ACCESS_KEY_ID=your_access_key_id`
   
`export AWS_SECRET_ACCESS_KEY=your_secret_access_key`

3) Then execute `source ~/.zprofile`
4) Build docker images: `cluster/build-docker.sh`
5) Run cluster: `cluster/start-cluster.sh`
6) Run spark-shell on cluster: `cluster/spark-shell-cluster.sh`
7) Stop cluster (also deletes all data): `cluster/stop-cluster.sh`

#### How to work with s3:

1) Start cluster and connect to it (see instructions above)

2) Execute (make sure there is `students-s3.csv` in the bucket) 
  
`spark.read.csv("s3a://genestack-spark-test/students-s3.csv").show`
