#!/usr/bin/env bash
#
# Script to create a Dataproc Cluster with internal IP Address
PROJECT="test-project"
REGION="us-central1"
ZONE="us-central1-b"
WORKERS=6
MASTERBOOTDISKSIZE=100
WORKERBOOTDISKSIZE=100
LOCALSSDS=1
IMAGEVERSION=1.3
MASTERMACHINE="n1-highmem-16"
WORKERMACHINE="n1-highmem-16"
SUBNETWORK="projects/<project>/regions/<region>/subnetworks/<subnetwork>"
INTERNALONLY=true
LABELS="key1=value1,key2=value2,..."
NETWORKTAGS="tag1"
DATABASE="dbname"
# Dataproc runs a local mysql on master. Use another port
LOCALDBPORT=33006
METADATA1="additional-cloud-sql-instances=${PROJECT}:${REGION}:${DATABASE}=tcp:${LOCALDBPORT}"
METADATA2="enable-cloud-sql-hive-metastore=false"
INITACTIONS="'gs://dataproc-initialization-actions/cloud-sql-proxy/cloud-sql-proxy.sh'"
SCOPES='https://www.googleapis.com/auth/cloud-platform'
SERVICEACCOUNT="serviceaccount@${PROJECT}.iam.gserviceaccount.com"

[ ${INTERNALONLY} == "true"] && { gcloud beta dataproc clusters create <cluster name> \
--region ${REGION} \
--zone ${ZONE} \
--subnet ${SUBNETWORK} \
--no-address \
--master-machine-type ${MASTERMACHINE} \
--master-boot-disk-size ${MASTERBOOTDISKSIZE} \
--num-workers ${WORKERS} \
--worker-machine-type ${WORKERMACHINE} \
--worker-boot-disk-size ${WORKERBOOTDISKSIZE} \
--num-worker-local-ssds ${LOCALSSDS} \
--image-version ${IMAGEVERSION} \
--scopes ${SCOPES} \
--labels ${LABELS} \
--tags ${NETWORKTAGS} \
--project ${PROJECT} \
--initialization-actions ${INITACTIONS} \
--metadata ${METADATA1} \
--metadata ${METADATA2} \
--service-account=${SERVICEACCOUNT}; }
