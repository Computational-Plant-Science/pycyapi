#!/bin/bash
#SBATCH --job-name=plantit_test
#SBATCH --time=00:10:00
#SBATCH --partition=normal
#SBATCH -c 1
#SBATCH -N 1
#SBATCH --ntasks=1
#SBATCH --mail-type=END,FAIL
#SBATCH --mail-user=wbonelli@uga.edu
#SBATCH --output=plantit_test.%j.out
#SBATCH --error=plantit_test.%j.err

echo "job planted!"