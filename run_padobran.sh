#!/bin/bash

#PBS -N PEADPREPAREMONTH
#PBS -l mem=80GB

cd ${PBS_O_WORKDIR}
apptainer run image.sif run_padobran.R
