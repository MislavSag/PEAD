#!/bin/bash

#PBS -N PEADPREPAREMONTH
#PBS -l mem=55GB

cd ${PBS_O_WORKDIR}
apptainer run image.sif run_padobran.R

