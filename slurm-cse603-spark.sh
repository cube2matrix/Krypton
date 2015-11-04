#!/bin/bash

### Make sure that nodes are exclusive for you
### ntasks-per-node is irrelevant
#SBATCH --exclusive
#SBATCH --ntasks-per-node=8

### Customize this section for your job
#SBATCH --nodes=4
#SBATCH --time=1:00:00

#SBATCH --mem=64000
#SBATCH --partition=debug
#SBATCH --job-name="Krypton_darrenxyli_xli66"
#SBATCH --output=%j.stdout
#SBATCH --error=%j.stderr
#SBATCH --mail-user=xli66@buffalo.edu
#SBATCH --mail-type=ALL

# --ntasks-per-node SETS NUMBER OF SPARK EXECUTORS
# executor_cores SETS NUMBER OF CORES PER EXECUTOR
executor_cores=4

# MAKE SURE THAT SPARK_LOG_DIR AND SPARK_WORKER_DIR
# ARE SET IN YOUR BASHRC, FOR EXAMPLE:
# export SPARK_LOG_DIR=/tmp
# export SPARK_WORKER_DIR=/scratch/xli66

# Add extra modules here
module load python/anaconda

# Set your command and arguments
PROG="/gpfs/user/xli66/Krypton/Krypton/main.py"
ARGS="32"


####### DO NOT EDIT THIS PART
module load java/1.8.0_45
module load hadoop/2.6.0
module load spark/1.4.1-hadoop

# GET LIST OF NODES
NODES=(`srun hostname | sort | uniq`)

NUM_NODES=${#NODES[@]}
LAST=$((NUM_NODES - 1))

# FIRST NODE IS MASTER
ssh ${NODES[0]} "cd $SPARK_HOME; ./sbin/start-master.sh"
MASTER="spark://${NODES[0]}:7077"

# ALL OTHER NODES ARE WORKERS
mkdir -p $SLURM_SUBMIT_DIR/$SLURM_JOB_ID
for i in `seq 1 $LAST`; do
  ssh ${NODES[$i]} "cd $SPARK_HOME; nohup ./bin/spark-class org.apache.spark.deploy.worker.Worker $MASTER &> $SLURM_SUBMIT_DIR/$SLURM_JOB_ID/nohup-${NODES[$i]}.out" &
done


# SUBMIT PYSPARK JOB
$SPARK_HOME/bin/spark-submit --master $MASTER $PROG $ARGS


# CLEAN SPARK JOB
ssh ${NODES[0]} "cd $SPARK_HOME; ./sbin/stop-master.sh"

for i in `seq 1 $LAST`; do
  ssh ${NODES[$i]} "killall java"
done
