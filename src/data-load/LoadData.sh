#!/bin/sh

gcloud functions deploy LoadData --region=europe-west2 --runtime go111 --trigger-topic cron-topic
