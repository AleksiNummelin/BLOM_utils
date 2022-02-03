#!/bin/bash
#
# Job name:
#SBATCH --job-name=nc3to4
# project
#SBATCH --account=nn9252k
#SBATCH --qos=preproc
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem-per-cpu=2GB
# Wall clock limit:
#SBATCH --time=02:00:00
#
set -o errexit  # Exit the script on any error
#set -o nounset  # Treat any unset variables as an error
#
module --quiet purge
#
workdir=$USERWORK/$SLURM_JOB_ID
mkdir -p $workdir
# change below, absolute path is needed
cp /cluster/home/anu074/BLOM_utils/fix_unlimited.py $workdir
cd $workdir
# NOTE activate environment
source /cluster/home/anu074/miniconda3/bin/activate python3env
python fix_unlimited.py
