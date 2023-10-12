#!/bin/bash

#PBS -N PEAD
#PBS -l ncpus=4
#PBS -l mem=25GB
#PBS -J 1-8695

cd ${PBS_O_WORKDIR}
apptainer run image.sif run_job.R
