### Feed source databases

For example:

``python insert_orders_chicago.py --host 34.66.161.43 -y 2020``

``insert_orders_la.py --host 34.66.161.43 --port 3307 -y 2020 -n 1000``


###Complie & Deploy Chicago

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

### Compile & Deploy LA

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