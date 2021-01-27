### Feed source databases

For example:

Insert records for chicago source database.

``python insert_orders_chicago.py --host 34.66.161.43 -y 2020``

or 

``Python3 insert_orders_chicago.py --host 34.66.161.43 -y 2020``

Insert records for LA source database.

``python insert_orders_la.py --host 34.66.161.43 --port 3307 -y 2020 -n 1000``

of 

``Python3 insert_orders_chicago.py --host 34.66.161.43 -y 2020``

## Compiling and Deploying your pipeline

Use `maven`

``mvn compile -X exec:java``

The arguments are:
* *Dexec.mainClass*: Java class with the main that receives arguments to constructs `PipelineOptions` instance.
* *runner*: Beam's supported runners: Direct (for local testing), Flink, Nemo, Samza, Spark, Dataflow.
* *project*: GCP project.
* *region*: GCP region to deploy to.
* *gcpTempLocation*: Temporary location where the compiled and packaged code will be copied before cluster deployment (when running on Dataflow).
* *username*: Debezium connector parameter used to connect to the source database.
* *password*: Debezium connector parameter used to connect to the source database.
* *hostname*: Debezium connector parameter used to connect to the source database.
* *port*: Debezium connector parameter used to connect to the source database.
* *serviceAccount*: GCP service account.
* *outputTable*: Pipeline parameter that establish the target table in GCP BigQuery.
* *bigQueryLoadingTemporaryDirectory*: GCP Temporary directory for BigQuery loading process.
* *defaultWorkerLogLevel*: Use this option to set all loggers at the specified default level.


**Chicago example**

``mvn compile -X exec:java \
-Dexec.mainClass=com.google.cloud.teleport.templates.ChicagoToBigQuery \
-Dexec.args="--runner=DataflowRunner \
--project=apache-beam-poc-4a7e4215 \
--region=us-central1 \
--gcpTempLocation=gs://gcp-beam-hassan-test/temp/ \
--username=debezium \
--password=dbz \
--hostname=34.66.161.43 \
--port=3306 \
--serviceAccount=gcp-beam-poc-hassan@apache-beam-poc-4a7e4215.iam.gserviceaccount.com \
--outputTable=apache-beam-poc-4a7e4215:google_beam_poc.merged_orders \
--bigQueryLoadingTemporaryDirectory=gs://gcp-beam-hassan-test/temp/ \
--defaultWorkerLogLevel=DEBUG"``

**LA example**

``mvn compile -X exec:java \
-Dexec.mainClass=com.google.cloud.teleport.templates.LosAngelesToBigQuery \
-Dexec.args="--runner=DataflowRunner \
--project=apache-beam-poc-4a7e4215 \
--region=us-central1 \
--gcpTempLocation=gs://gcp-beam-hassan-test/temp/ \
--username=debezium \
--password=dbz \
--hostname=34.66.161.43 \
--port=3307 \
--serviceAccount=gcp-beam-poc-hassan@apache-beam-poc-4a7e4215.iam.gserviceaccount.com \
--outputTable=apache-beam-poc-4a7e4215:google_beam_poc.merged_orders \
--bigQueryLoadingTemporaryDirectory=gs://gcp-beam-hassan-test/temp/ \
--defaultWorkerLogLevel=DEBUG"``