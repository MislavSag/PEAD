#!/bin/bash

#PBS -N PEAD
#PBS -l ncpus=4
#PBS -l mem=15GB
#PBS -J 1-434
#PBS -o experiments/logs
#PBS -j oe

cd ${PBS_O_WORKDIR}
apptainer run image.sif run_job.R


