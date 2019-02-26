#!/bin/bash
#Created By: Praful Kumar Bhise<PKB017@MAERSK.COM>
#use today's date and time
#######################################################################################################
day=$(date +%Y%m%d%H%M%S)

#Parameter List
BUCKET=$1
TARGET_FOLDER=$2
TARGET_HDP_FOLDER=$3
SOURCE_NAME=$4

echo "Connected to Azure - Initiating the process now"
export PATH=~/.local/bin/:$PATH

TARGET_FILE_PATH=$TARGET_FOLDER/inttra_stage/
log_dir=$TARGET_FOLDER/extractLogs
fileDateChecker_txt=$TARGET_FOLDER/fileDateChecker.txt
fileCopyCount=0

##Read date from file
val_fileDateChecker=$(head -n 1 $fileDateChecker_txt)
if [[ -n "$val_fileDateChecker" ]]; then
        fileDateChecker=$(echo $val_fileDateChecker | sed 's/|.*\(.*\)/\1/')
else
        fileDateChecker="20000101000000"
fi
echo "INFO: Processing copy for files greater than date(YYYYMMDDHHMISS) - "$fileDateChecker

## copy files to target location
for d in $(aws s3 ls $BUCKET | awk '{print $1$2 "|" $4}')
   do
        awsFileName=$(basename $d)
        v_fileDateChecker=$(echo $awsFileName | sed 's/|.*\(.*\)/\1/')
        v_awsFileName=$(echo $awsFileName | sed 's/-//g;s/\://g' | awk {'if ($1-'$fileDateChecker' > 0 ) {print $1}'})
        if [[ -n "$v_awsFileName" ]]; then
                p_awsfName=$(echo $awsFileName | sed 's/.*|\(.*\)/\1/')
                aws s3 cp $BUCKET$p_awsfName $TARGET_FILE_PATH$p_awsfName
                echo "INFO: File "$p_awsfName " copied succesfully"
                fileCopyCount=$((fileCopyCount+1))
        else
                echo "INFO: File "$awsFileName "extracted earlier, ignoring now."
        fi
done

echo "INFO: Moving files from tmp/data to tmp/archive folder"
hadoop fs -mv $TARGET_HDP_FOLDER/data/$SOURCE_NAME/* $TARGET_HDP_FOLDER/archive/$SOURCE_NAME/
echo "INFO: Moving files completed to archive folder"

##Write Date to file
#cd $TARGET_FILE_PATH
if [[ "$fileCopyCount" > 0 ]]; then
        #Updating the
        processedDName=$(find $TARGET_FILE_PATH -type f -printf '%TY%Tm%Td%TH%TM%TS %p\n' | sort -n | tail -1 | awk '{print $1 "|" $2}')
        echo $processedDName > $fileDateChecker_txt
        ##Copy files to Hadoop tmp location
        hadoop fs -moveFromLocal $TARGET_FILE_PATH* $TARGET_HDP_FOLDER/data/$SOURCE_NAME/
else
echo "No new files to be copied or all files available in aws s3 are older than date."
fi

echo "SUMMARY: Total files copied and loaded to hadoop-tmp: "$fileCopyCount " "
echo "INFO: Finished Processing the copy load request."
echo "INFO: Starting incremental for Port Codes table"
sh -x /home/aa-crbprdadldl1/datalake_scripts/inttra/bin/sqoop_ingestion_inttra.sh
        error_code=$?
                if [ $error_code == 0 ];then
                echo "INFO:$(date -d "today" +"%Y:%m:%d:%H:%M"): Port Codes table has been updated successfully"
                exit 0
        else
                echo "ERROR:$(date -d "today" +"%Y:%m:%d:%H:%M"): Port Codes table has not updated, Please check for the root cause"
                echo "Failed to update Port Codes table, Please check" | mailx -s "Inttra Failure Alert: Failed to update Port Codes table" -r "pkb017@maersk.com" "pkb017@maersk.com,BIDataOperationsSupport@maersk.com"
                exit 1
                fi
