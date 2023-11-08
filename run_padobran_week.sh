#!/bin/bash

#PBS -N PEADPREPARE
#PBS -l mem=55GB

cd ${PBS_O_WORKDIR}
apptainer run image.sif run_padobran_week.R
