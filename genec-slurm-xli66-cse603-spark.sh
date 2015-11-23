#!/bin/bash

####### DO NOT EDIT THIS SECTION
#######SBATCH --partition=general-c
#SBATCH --exclusive

####### CUSTOMIZE THIS SECTION FOR YOUR JOB
####### KEEP --mem=64000 TO USE FULL MEMORY
#SBATCH --mem=64000
#SBATCH --job-name="Krypton"
#SBATCH --nodes=8
#SBATCH --ntasks-per-node=8
#SBATCH --output=%j.stdout
#SBATCH --error=%j.stderr
#SBATCH --time=02:00:00
#SBATCH --mail-user=xli66@buffalo.edu
#SBATCH --mail-type=ALL

# --ntasks-per-node SETS NUMBER OF SPARK EXECUTORS
# executor_cores SETS NUMBER OF CORES PER EXECUTOR
executor_cores=8

# MAKE SURE THAT SPARK_LOG_DIR AND SPARK_WORKER_DIR
# ARE SET IN YOUR BASHRC, FOR EXAMPLE:
export SPARK_LOG_DIR=/tmp/xli66/log
export SPARK_WORKER_DIR=/tmp/xli66/work
export SPARK_LOCAL_DIRS=$SPARK_WORKER_DIR
mkdir -p $SPARK_LOG_DIR $SPARK_WORKER_DIR

# ADD EXTRA MODULES HERE IF NEEDED
module load python/anaconda

# SET YOUR COMMAND AND ARGUMENTS
PROG="/gpfs/user/xli66/Krypton/graph_generator/main.py"
ARGS="32"


####### DO NOT EDIT THIS PART
module load java/1.8.0_45
module load hadoop/2.6.0
module load spark/1.4.1-hadoop

# GET LIST OF NODES
NODES=(`srun hostname | sort`)

NUM_NODES=${#NODES[@]}
LAST=$((NUM_NODES - 1))

# FIRST NODE IS MASTER
ssh ${NODES[0]} "cd $SPARK_HOME; ./sbin/start-master.sh"
MASTER="spark://${NODES[0]}:7077"

# ALL OTHER NODES ARE WORKERS
mkdir -p $SLURM_SUBMIT_DIR/$SLURM_JOB_ID
for i in `seq 0 $LAST`; do
  ssh ${NODES[$i]} "cd $SPARK_HOME; nohup ./bin/spark-class org.apache.spark.deploy.worker.Worker $MASTER &> $SLURM_SUBMIT_DIR/$SLURM_JOB_ID/nohup-${NODES[$i]}.$i.out" &
done

# SUBMIT PYSPARK JOB
$SPARK_HOME/bin/spark-submit --conf spark.akka.frameSize=128 --driver-memory 4g --executor-memory 8g --executor-cores $executor_cores --py-files //gpfs//scratch//xli66//nltk.zip --master $MASTER $PROG $ARGS

# CLEAN SPARK JOB
ssh ${NODES[0]} "cd $SPARK_HOME; ./sbin/stop-master.sh"

for i in `seq 0 $LAST`; do
  ssh ${NODES[$i]} "killall java"
done
