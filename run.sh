#!/bin/bash

#PBS -N PEAD
#PBS -l ncpus=4
#PBS -l mem=2GB
#PBS -J 1-2670

cd ${PBS_O_WORKDIR}
apptainer run image.sif run_job.R
