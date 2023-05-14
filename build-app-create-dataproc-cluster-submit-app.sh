#!/bin/bash

JAVA_HOME=$(/usr/libexec/java_home -v11)
export JAVA_HOME=$JAVA_HOME
export PATH=$JAVA_HOME/bin:$PATH
mvn clean package -Pdist -Dspark.version=3.3.0 -Djava.version=11

JAR=$(ls target/spark-app-dataproc*.jar)
echo "JAR=$JAR"

# Place your project details
#export GCP_PROJECT=my-project
#export GCP_REGION=us-central1
#export GCP_ZONE=us-central1-b
#export GCP_SUBNETWORK=https://www.googleapis.com/compute/v1/projects/my-network-project/regions/us-central1/subnetworks/my-subnetwork
#export GCP_SERVICE_ACCOUNT=my-service-account@my-project.iam.gserviceaccount.com

SOURCE_TABLE="bartek_person.bartek_person_spark"
TARGET_TABLE="bartek_person.bartek_person_spark"
BUCKET="${GCP_PROJECT}-bartek-dataproc"
CLUSTER=bartek-spark-330s-on-dataproc

#gcloud dataproc clusters delete ${CLUSTER} --project ${GCP_PROJECT} --region us-central1 --quiet
#gsutil -m rm -r gs://${GCP_PROJECT}-${CLUSTER}
#gsutil mb -l ${GCP_REGION} gs://${GCP_PROJECT}-${CLUSTER}
#
#gcloud dataproc clusters create ${CLUSTER} \
#--project ${GCP_PROJECT} --region us-central1 --zone="" --no-address \
#--subnet ${GCP_SUBNETWORK} \
#--master-machine-type t2d-standard-4 --master-boot-disk-size 1000 \
#--num-workers 2 --worker-machine-type t2d-standard-4 --worker-boot-disk-size 2000 \
#--image-version 2.1.11-debian11 \
#--scopes 'https://www.googleapis.com/auth/cloud-platform' \
#--service-account=${GCP_SERVICE_ACCOUNT} \
#--bucket ${GCP_PROJECT}-${CLUSTER} \
#--optional-components DOCKER \
#--enable-component-gateway \
#--properties spark:spark.master.rest.enabled=true,dataproc:dataproc.logging.stackdriver.job.driver.enable=true,dataproc:dataproc.logging.stackdriver.enable=true,dataproc:jobs.file-backed-output.enable=true,dataproc:dataproc.logging.stackdriver.job.yarn.container.enable=true \
#--metric-sources=spark,hdfs,yarn,spark-history-server,hiveserver2,hivemetastore,monitoring-agent-defaults

echo "${SOURCE_TABLE} content"
bq show "${SOURCE_TABLE}"
bq head --max_rows=3 "${SOURCE_TABLE}"

echo "${TARGET_TABLE} content"
bq show "${TARGET_TABLE}"
bq head --max_rows=3 "${TARGET_TABLE}"

gcloud dataproc jobs submit spark --cluster=${CLUSTER} --region=us-central1 \
--class=com.bartek.spark.SparkApp \
--jars="${JAR}" \
--labels=job_name=bartek-sparkapp \
--properties ^#^spark.jars.packages=org.apache.spark:spark-avro_2.12:3.3.0,com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.27.1#spark.dynamicAllocation.enabled=true#spark.shuffle.service.enabled=true \
-- \
 projectId=${GCP_PROJECT} \
 bucket="${BUCKET}" \
 sourceTable="${SOURCE_TABLE}" \
 targetTable="${TARGET_TABLE}"

echo "Waiting 10 secs for logs to appear in GCP Logs Explorer"
SLEEP 10

LABELS_JOB_NAME=bartek-sparkapp && \
START_TIME="$(date -u -v-1S '+%Y-%m-%dT%H:%M:%SZ')" && \
END_TIME="$(date -u -v-10M '+%Y-%m-%dT%H:%M:%SZ')" && \
LATEST_JOB_ID=$(gcloud dataproc jobs list --region=us-central1 --filter="placement.clusterName=${CLUSTER} AND labels.job_name=${LABELS_JOB_NAME}" --format=json --sort-by=~status.stateStartTime | jq -r ".[0].reference.jobId") && \
echo "Latest job id: $LATEST_JOB_ID" &&
gcloud logging read --project ${GCP_PROJECT} "timestamp<=\"${START_TIME}\" AND timestamp>=\"${END_TIME}\" AND resource.type=cloud_dataproc_job AND labels.\"dataproc.googleapis.com/cluster_name\"=${CLUSTER} AND resource.labels.job_id=${LATEST_JOB_ID} AND severity>=INFO" --format "table(timestamp,jsonPayload.message)" --order=asc
