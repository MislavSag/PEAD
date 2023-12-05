#!/bin/bash

#PBS -N PEAD
#PBS -l ncpus=4
#PBS -l mem=8GB
#PBS -J 1-10000
#PBS -o experimentsmonth/logs
#PBS -j oe

cd ${PBS_O_WORKDIR}
apptainer run image.sif run_job.R

# 1-16456
