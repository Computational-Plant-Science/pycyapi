#!/bin/bash
#SBATCH --partition=normal
#SBATCH --job-name=PlantIT
#SBATCH --ntasks=1
#SBATCH --nodes=1
#SBATCH --mem=2G
#SBATCH --time=00:10:00
#SBATCH --mail-user=wbonelli@uga.edu
#SBATCH -A TG-BIO160088

export LC_ALL=en_US.utf8
export LANG=en_US.utf8
