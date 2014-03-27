#!/bin/bash

# Created by Siyuan Guo, Mar 2014

# Shell script to run mapreduce codes on FutureGrid

# Put this shell script in your $HOME directory

#PBS -q batch
#PBS -N HTRC_date
#PBS -l nodes=4:ppn=8
#PBS -o HTRCDatePBS.log
#PBS -j oe
#PBS -V

echo "Loading modules"
module add java
module add python
echo

echo "Error when 'java -version' but not when 'java -Xmx256m -version'"
java -version
java -Xmx1024m -version
echo

echo "What's wrong?"
cat /proc/meminfo
echo
ulimit -a
echo

### Run the myHadoop environment script to set the appropriate variables
#
# Note: ensure that the variables are set correctly in bin/setenv.sh
. /N/soft/myHadoop/bin/setenv.sh

#### Set this to the directory where Hadoop configs should be generated
# Don't change the name of this variable (HADOOP_CONF_DIR) as it is
# required by Hadoop - all config files will be picked up from here
#
# Make sure that this is accessible to all nodes
export HADOOP_CONF_DIR="$HOME/myHadoop-config"

# Make sure number of nodes is the same as what you have requested from PBS
# usage: $MY_HADOOP_HOME/bin/pbs-configure.sh -h
echo "Set up the configurations for myHadoop"
# this is the non-persistent mode
$MY_HADOOP_HOME/bin/pbs-configure.sh -n 4 -c $HADOOP_CONF_DIR
# this is the persistent mode
# $MY_HADOOP_HOME/bin/pbs-configure.sh -n 4 -c $HADOOP_CONF_DIR -p -d /oasis/cloudstor-group/HDFS
echo

#### Format HDFS, if this is the first time or not a persistent instance
echo "Format HDFS"
$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR namenode -format
echo

#### Start the Hadoop cluster
echo "Start all Hadoop daemons"
$HADOOP_HOME/bin/start-all.sh
#$HADOOP_HOME/bin/hadoop dfsadmin -safemode leave
echo

#### Make sure mapper.p & reducer.py are executable
export MAPREDUCE_CODES_PATH="$HOME/Z604-Project/text_processing/mapreduce/getDateInText"
# export MAPREDUCE_CODES_PATH="$HOME/Z604-Project/text_processing/mapreduce/getTF"
chmod +x $MAPREDUCE_CODES_PATH/mapper.py
chmod +x $MAPREDUCE_CODES_PATH/reducer.py

#### Run your jobs here
echo "Run some test Hadoop jobs"
$HADOOP_HOME/bin/hadoop fs -mkdir HTRCInputFiles
$HADOOP_HOME/bin/hadoop fs -put $HOME/HTRCInputFiles/* HTRCInputFiles/
$HADOOP_HOME/bin/hadoop jar -Xmx1024m $HADOOP_HOME/contrib/streaming/hadoop-0.20.2-streaming.jar \
	-file $MAPREDUCE_CODES_PATH/mapper.py \
	-mapper $MAPREDUCE_CODES_PATH/mapper.py \
	-file $MAPREDUCE_CODES_PATH/reducer.py \
	-reducer $MAPREDUCE_CODES_PATH/reducer.py \
	-input HTRCInputFiles \
	-output HTRCDateOutputFiles \

# jar /opt/hadoop-0.20.2/contrib/streaming/hadoop-0.20.2-streaming.jar -file /N/u/zachguo/Z604-Project/text_processing/getDateInText/mapper.py -mapper /N/u/zachguo/Z604-Project/text_processing/getDateInText/mapper.py -file /N/u/zachguo/Z604-Project/text_processing/getDateInText/reducer.py -reducer /N/u/zachguo/Z604-Project/text_processing/getDateInText/reducer.py -input HTRCInputFiles/* -output HTRCDateOutputFiles/

echo "Copy output data back to local filesystem"
$HADOOP_HOME/bin/hadoop fs -copyToLocal HTRCDateOutputFiles/* $HOME/HTRCDateOutputFiles/
echo

#### Stop the Hadoop cluster
echo "Stop all Hadoop daemons"
$HADOOP_HOME/bin/stop-all.sh
echo

#### Clean up the working directories after job completion
echo "Clean up"
$MY_HADOOP_HOME/bin/pbs-cleanup.sh -n 4 -c $HADOOP_CONF_DIR
echo

#### Run the script and check status
# qsub HTRC_date_streaming.sh
# showq
# qstat
