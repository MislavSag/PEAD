#!/bin/bash

#PBS -N PEAD
#PBS -l ncpus=4
#PBS -l mem=8GB
#PBS -J 1-11
#PBS -o experiments_pread_live/logs
#PBS -j oe

cd ${PBS_O_WORKDIR}
apptainer run image.sif run_job.R 0 experiments_pread_live
