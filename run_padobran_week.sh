#!/bin/bash

#PBS -N PEADPREPARE
#PBS -l mem=35GBs

cd ${sPBS_O_WORKDIR}
apptainer run image.sif run_padobran_week.R
