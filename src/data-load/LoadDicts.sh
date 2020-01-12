#!/bin/sh

gcloud functions deploy LoadDicts --region=europe-west2 --runtime go111 --trigger-resource pps --trigger-event google.storage.object.finalize
