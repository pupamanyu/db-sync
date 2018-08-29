#!/usr/bin/env bash
#
# Script to delete the Dataproc Cluster
REGION="us-central1"
CLUSTERNAME="test-cluster"
gcloud beta dataproc clusters delete --region ${REGION}
