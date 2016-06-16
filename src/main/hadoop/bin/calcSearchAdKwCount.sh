#!/bin/bash
#
# This script is kicked off by Azkaban as part of the Popular Search flow
#
# Usage: ./calcSearchAdKwCoung.sh -i <input_dir> -o <output_dir> -d <deploy_dir> -a <days_ago> -b <days_back>
#
#        input_dir:  HDFS directory for input data
#        output_dir: HDFS directory for output data
#        deploy_dir: deploydirectory (to find pig scripts and jar files)
#        days_ago:   from how many days ago do you want to start the calculation
#        days_back:  how many days from days_ago do you want to apply the calculation
#

DAYS_AGO=1
INPUT_DIR=/user/data/bolt/data/qa/input
OUTPUT_DIR=/user/data/bolt/data/qa/daily/popular_search
DEPLOY_DIR=~
NUM_DAYS_BACK=30

while getopts "i:o:d:a:b:" option
do
        case "${option}" in

                i) INPUT_DIR=${OPTARG};;
                o) OUTPUT_DIR=${OPTARG};;
                d) DEPLOY_DIR=${OPTARG};;
                a) DAYS_AGO=${OPTARG};;
                b) NUM_DAYS_BACK=$OPTARG;;
        esac
done

echo ===== Running with the following parameters =====
echo ===== DAYS_AGO:            $DAYS_AGO
echo ===== INPUT_DIR:           $INPUT_DIR
echo ===== OUTPUT_DIR:          $OUTPUT_DIR
echo ===== DEPLOY_DIR:          $DEPLOY_DIR
echo ===== NUM_DAYS_BACK:       $NUM_DAYS_BACK
echo =================================================

# Set how many days ago you want to calculate from today
daysago=$DAYS_AGO

# Base directory where everything is read or stored
inputdir=$INPUT_DIR
outputdir=$OUTPUT_DIR

# Base deploy dir for all pig scripts
deploydir=$DEPLOY_DIR

# Calculate for NUM_DAYS_BACK days
for ((var=0;var<$NUM_DAYS_BACK;++var));
do
  setday=$((var + $daysago))
  thedate=`date --date="$setday day ago" +%Y%m%d`;
  echo $thedate

  echo hadoop fs -rmr $outputdir/$thedate/searchview_kwcount 
  hadoop fs -rmr $outputdir/$thedate/searchview_kwcount 
  hadoop fs -rmr $outputdir/$thedate/sv_filtered 

  echo pig -Dpig.skewedjoin.reduce.memusage=0.1 -param deploydir=$deploydir -param outputdir=$outputdir -param inputdir=$inputdir -param daydate=$thedate -f $deploydir/pig/searchViewCount.pig
  pig -Dpig.skewedjoin.reduce.memusage=0.1 -param deploydir=$deploydir -param outputdir=$outputdir -param inputdir=$inputdir -param daydate=$thedate -f $deploydir/pig/searchViewCount.pig
done

