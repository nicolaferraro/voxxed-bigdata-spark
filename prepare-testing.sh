#!/bin/bash

oc new-project testing
oc create -f http://radanalytics.io/resources.yaml

# Adding grant to the "developer" user
oc adm policy add-role-to-user edit developer
