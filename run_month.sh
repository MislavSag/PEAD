#!/bin/bash

#PBS -N PEAD
#PBS -l ncpus=4
#PBS -l mem=32GB
#PBS -J 1-56
#PBS -o experimentsmonth/logs
#PBS -j oe

cd ${PBS_O_WORKDIR}
apptainer run image.sif run_job.R 1

# 1-16456
# 16456 - 16402
