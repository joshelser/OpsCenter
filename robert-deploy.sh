#!/bin/bash

set -ex

OPSCENTER_PACKAGE=JOSH_OCWH_PACKAGE OPSCENTER_DATABASE=JOSH_OCWH_DB OPSCENTER_APP=WH_SCHED_OPSCENTER poetry run python deploy/deploy.py -v 0 -p whsched_app -d dev
