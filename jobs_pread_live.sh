#!/bin/bash

#PBS -N PEAD
#PBS -l ncpus=4
#PBS -l mem=16GB
#PBS -J 1-6
#PBS -o experiments_pread_live/logs
#PBS -j oe

cd ${PBS_O_WORKDIR}
apptainer run image.sif run_job.R 1 experiments_pread_live

# 11 - 5
