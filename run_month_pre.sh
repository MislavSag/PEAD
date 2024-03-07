#!/bin/bash

#PBS -N PEAD
#PBS -l ncpus=4
#PBS -l mem=8GB
#PBS -J 1-583
#PBS -o experiments_pre/logs
#PBS -j oe

cd ${PBS_O_WORKDIR}
apptainer run image.sif run_job_pre.R 0 experiments_pre
