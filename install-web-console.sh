#!/bin/bash

oc create -f ./oshinko-resources.yaml
oc new-app oshinko-webui
