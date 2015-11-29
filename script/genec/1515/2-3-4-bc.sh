#!/bin/bash

####### DO NOT EDIT THIS SECTION
#######SBATCH --partition=general-c
#SBATCH --exclusive

####### CUSTOMIZE THIS SECTION FOR YOUR JOB
####### KEEP --mem=64000 TO USE FULL MEMORY
#SBATCH --mem=80000
#SBATCH --job-name="Krypton"
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=3
#SBATCH --output=%j.stdout
#SBATCH --error=%j.stderr
#SBATCH --time=06:00:00
#SBATCH --mail-user=xli66@buffalo.edu
#SBATCH --mail-type=ALL
#SBATCH --requeue

# --ntasks-per-node SETS NUMBER OF SPARK EXECUTORS
# executor_cores SETS NUMBER OF CORES PER EXECUTOR
executor_cores=4

# MAKE SURE THAT SPARK_LOG_DIR AND SPARK_WORKER_DIR
# ARE SET IN YOUR BASHRC, FOR EXAMPLE:
export SPARK_LOG_DIR=/tmp/xli66/log
export SPARK_WORKER_DIR=/tmp/xli66/work
export SPARK_LOCAL_DIRS=$SPARK_WORKER_DIR
mkdir -p $SPARK_LOG_DIR $SPARK_WORKER_DIR

# ADD EXTRA MODULES HERE IF NEEDED
module load python/anaconda

# SET YOUR COMMAND AND ARGUMENTS
PROG="/user/xli66/Krypton/Krypton/target/scala-2.10/krypton_2.10-0.1.jar"
ARGS="/gpfs/courses/cse603/students/xli66/603/graph/1515lines/edges/part-00000"
PNUM="12"

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
  ssh ${NODES[$i]} "cd $SPARK_HOME;  nohup ./bin/spark-class org.apache.spark.deploy.worker.Worker $MASTER &> $SLURM_SUBMIT_DIR/$SLURM_JOB_ID/nohup-${NODES[$i]}.$i.out" &
done

# SUBMIT PYSPARK JOB
$SPARK_HOME/bin/spark-submit --conf spark.akka.frameSize=512 --conf spark.storage.memoryFraction=0.3 --conf spark.shuffle.memoryFraction=0.1 --conf spark.default.parallelism=$PNUM --conf spark.driver.maxResultSize=5g --num-executors 5 --driver-memory 5g --executor-memory 4g --executor-cores $executor_cores --jars $(echo /user/xli66/Krypton/Krypton/lib/*.jar | tr ' ' ',') --master $MASTER $PROG $ARGS $PNUM

# CLEAN SPARK JOB
ssh ${NODES[0]} "cd $SPARK_HOME; ./sbin/stop-master.sh"

for i in `seq 0 $LAST`; do
  ssh ${NODES[$i]} "killall java"
done
