#!/usr/bin/env bash
#
# Script to run the MySQL DB Migration Job
SPARKMASTER="yarn-cluster"
# Set Dataproc Cluster Name
CLUSTER=<dataproc cluster name>
# Set App Name
APPNAME="MySQL Full Migration Test"
# Set Dataproc Region
REGION=<dataproc region>
SPARKSERIALIZER="org.apache.spark.serializer.KryoSerializer"
# Set Job Jar Location on GCS
JOBJAR="gs://<bucket>/db-sync-assembly-1.0.jar"
# Set Table Name
TABLE="<table name>"
# Set Job Configuration JSON Location
CONFIGFILE="gs://<bucket>/full-load-${TABLE}.json"
# Set Number of Executors
# NUMEXECUTORS=126
# Set Executor Memory
EXECUTORMEMORY="23g"
# Set Executor Cores per Node
EXECUTORCORES=3
# Set Driver Memory
DRIVERMEMORY="32g"
# Set Driver Cores
DRIVERCORES=8
# Set Maximum Cores per Cluster when Dynamic Allocation is disabled
#MAXCORES=126
# Enable/Disable OFFHEAP
OFFHEAPENABLED=false
# Set OFFHEAPSIZE only when OFFHEAP is enabled
OFFHEAPSIZE="30g"
# Set Memory Overhead Allocation
MEMORYOVERHEAD=600
# Enable/Disable Dynamic Allocation
DYNAMICALLOCATION=true
#
#
# Run the Spark Job
gcloud dataproc jobs submit spark \
  --cluster=${CLUSTER} \
  --region=${REGION} \
  --properties="spark.app.name=${APPNAME},spark.serializer=${SPARKSERIALIZER},spark.driver.cores=${DRIVERCORES},spark.driver.memory=${DRIVERMEMORY},spark.executor.memory=${EXECUTORMEMORY},spark.executor.cores=${EXECUTORCORES},spark.dynamicAllocation.enabled=${DYNAMICALLOCATION}" \
  --jar=${JOBJAR} -- ${CONFIGFILE}
