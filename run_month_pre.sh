#!/bin/bash

#PBS -N PEAD
#PBS -l ncpus=4
#PBS -l mem=16GB
#PBS -J 1-91
#PBS -o experiments_pre/logs
#PBS -j oe


cd ${PBS_O_WORKDIR}
apptainer run image.sif run_job.R 1 experiments_pre
