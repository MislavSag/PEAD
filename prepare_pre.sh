#!/bin/bash

#PBS -N PREPREPARE
#PBS -l mem=80GB

cd ${PBS_O_WORKDIR}
apptainer run image.sif prepare_pre.R 1
