#!/bin/bash

oc new-project testing
oc create -f ./oshinko-resources.yaml
oc new-app oshinko-webui
oc adm policy add-role-to-user edit developer
