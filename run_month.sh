
#PBS -N PEAD
#PBS -l ncpus=4
#PBS -l mem=10GB
#PBS -J 10000-10000
#PBS -o experimentsmonth/logs
#PBS -j oe

cd ${PBS_O_WORKDIR}
apptainer run image.sif run_job.R 0

# 1-18720
