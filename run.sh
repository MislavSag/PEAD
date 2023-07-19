#!/bin/bash

#PBS -l ncpus=8
#PBS -J 1-80

cd ${PBS_O_WORKDIR}
apptainer run image.sif run.R