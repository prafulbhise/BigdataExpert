#!/bin/bash
######################################################################################################################################################################
#  Script Name:   dl_file_transfer.sh
#  Author Name :  Praful Kumar Bhise<praful.bigdata@gmail.com>
#  Description:   generic file transfer framework for flat files from INFA unix systems to ADLS
#=========================================================================================================================                                              #
# Modification History:                                                                                                                                                 #
# Date :        Changed by :            Description :                                                                                                                   #
# ---------- ------------------ ------------------------------------------------------------------------------------------                                              #
#######################################################################################################################################################################
export script_dir='/opt/informatica/infa_shared/BDMshared/scripts/DataLake/generic/fileIngestion'
. $script_dir/global_variable_import.properties
application=$1
file_type=$2
#filename_pattern=$3
date_part=`date +%Y-%m-%d`
#check_dt="`date +"%b %d"`"

log_filename="${script_dir}/logs/${application}_$(date -d "today" +"%Y%m%d%H").log"

if [ -z $application ];then
        echo "Please provide input arguments correctly!" | mailx -s "${application} Failure Alert: FILE TRANSFER SCRIPT FAILED"   "praful.bigdata@gmail.com"
        exit 1
fi

parameter_parser()
{
  IFS=',' read -a array <<< $line
   actualfilename_pattern=${array[0]}
   tablename=${array[1]}
   source_dir=${array[2]}
   target_dir=${array[3]}


}

  echo "#################FILE TRANSFER START########################" >> $log_filename
   echo "INFO:$(date -d "today" +"%Y-%m-%d:%H:%M"): Initiating Transfer job" >> $log_filename
   echo "INFO:$(date -d "today" +"%Y-%m-%d:%H:%M"): Reading a control file in an array" >> $log_filename
   echo "INFO:$(date -d "today" +"%Y-%m-%d:%H:%M"): File transfer is initiated for a application - ${application}" >> $log_filename

sftp_files() {
sftp ${userid}@${hs_cluster} /bin/bash << EOF
cd $staging_dir/$application/data/
rm $staging_dir/$application/data/*
put $file_name
bye
EOF
}

console_tranfer()
{
sed 1d ${script_dir}/${application}.ctrl|while read -r line
        do
parameter_parser
if [ "$file_type" == "consolidated" ];then
        ssh ${userid}@${hs_cluster} /bin/bash << EOF
        cd ${staging_dir}/${application}/data/
        hadoop fs -conf ${adls_conf} -mkdir -p ${target_dir}/${tablename}/date_part=$date_part;
        hadoop fs -conf ${adls_conf} -put ${actualfilename_pattern}* ${target_dir}/${tablename}/date_part=${date_part}/;
        rm -f ${actualfilename_pattern}*
EOF
echo "INFO:$(date -d "today" +"%Y-%m-%d:%H:%M"): File Transfer success for  ${actualfilename_pattern}* actual file(s) on ADLS.." >> $log_filename
fi
done
}

consolidated_tranfer()
{
if [ "$file_type" == "consolidated" ];then
        l_date=`cat ${script_dir}/work/${application}/${application}_last_file.check |awk '{print $1}'`
        l_hour=`cat ${script_dir}/work/${application}/${application}_last_file.check | awk '{print $2}' | awk -F':' '{print $1}'`
        l_min=`cat ${script_dir}/work/${application}/${application}_last_file.check | awk '{print $2}' | awk -F':' '{print $2}'`
        if [ $l_min -eq "59" ];then
        l_hour=`expr $l_hour + 1`
        increment_min=`echo "00"`
        else
        l_hour=`echo $l_hour`
        increment_min=`expr $l_min + 1`
        fi
        last_processed_time="`echo $l_date $l_hour:$increment_min`"
        cd $source_dir
        check_file=`find * -type f -newermt "$last_processed_time" | wc -l`
        if [ "$check_file" == 0 ]; then
        echo "WARN:$(date -d "today" +"%Y-%m-%d:%H:%M"): No new files found to tranfer - Please verify if you are expecting something to be tranferred" >> $log_filename
        exit 0
        else
        for file_name in $(find * -type f -newermt "$last_processed_time");do
        echo $file_name
        echo "INFO:$(date -d "today" +"%Y-%m-%d:%H:%M"): New Files found: $file_name" >> $log_filename
        echo "INFO:$(date -d "today" +"%Y-%m-%d:%H:%M"): Initiating File Transfer Process for  $file_name consolidated file.." >> $log_filename
#Tranfer files to CDH cluster
        sftp_files
                check_status=$?
                if [ $check_status != 0 ];then
                         echo "ERROR: File Tranfer Failed, Please ensure the correct parameters are specified"
                         exit 1
                fi
        echo "INFO:$(date -d "today" +"%Y-%m-%d:%H:%M"): File Transfer success for  $file_name consolidated file on CDH.." >> $log_filename
        mkdir -p ${script_dir}/work/$application

        ssh ${userid}@${hs_cluster} /bin/bash << EOF
        rm -f $staging_dir/$application/archive/${actualfilename_pattern}*
        cd $staging_dir/$application/data/
        unzip $file_name
        mv $file_name $staging_dir/$application/archive/
EOF

        cd $source_dir
#       date_part=`ls -ltr --time-style=long-iso $file_name |  awk {'print $6'}`
        ls -ltr --time-style=long-iso $file_name |  awk {'print $6,$7,$8'} > ${script_dir}/work/$application/${application}_last_file.check
        console_tranfer
done
fi
fi
}

file_transfer()
{
if [ -z $file_type ];then
        cd $source_dir
        l_date=`cat ${script_dir}/work/${application}/${application}_${tablename}_last_file.check |awk '{print $1}'`
        l_hour=`cat ${script_dir}/work/${application}/${application}_${tablename}_last_file.check | awk '{print $2}' | awk -F':' '{print $1}'`
        l_min=`cat ${script_dir}/work/${application}/${application}_${tablename}_last_file.check | awk '{print $2}' | awk -F':' '{print $2}'`
        if [ $l_min -eq "59" ];then
        l_hour=`expr $l_hour + 1`
        increment_min=`echo "00"`
        else
        l_hour=`echo $l_hour`
        increment_min=`expr $l_min + 1`
        fi
        last_processed_time="`echo $l_date $l_hour:$increment_min`"
        cd $source_dir
        check_file=`find ${actualfilename_pattern}* -type f -newermt "$last_processed_time" | wc -l`
        if [ "$check_file" == 0 ]; then
        echo "WARN:$(date -d "today" +"%Y-%m-%d:%H:%M"): No new files found to tranfer - Please verify if you are expecting something to be tranferred" >> $log_filename
        else
        for file_name in $(find ${actualfilename_pattern}* -type f -newermt "$last_processed_time");do
        echo $file_name
        check_empty=`ls -lrt $file_name | awk {'print $5'}`
        if [ $check_empty -gt 0 ];then
        #date_part=`ls -ltr --time-style=long-iso $file_name |  awk {'print $6'}`
        echo "INFO:$(date -d "today" +"%Y-%m-%d:%H:%M"): New File Name: $file_name" >> $log_filename
        echo "INFO:$(date -d "today" +"%Y-%m-%d:%H:%M"): Initiating File Transfer Process for  $file_name gzip file.." >> $log_filename
        sftp_files
        ssh_status=$?
        if [ $ssh_status != 0 ];then
        echo "File Transfer Failed for $file_name file, please check connectivity issue from INFA TO CDH" | mailx -s "${application} Failure Alert: FILE TRANSFER SCRIPT FAILED"   "praful.bigdata@gmail.com"
        else
        echo "INFO:$(date -d "today" +"%Y-%m-%d:%H:%M"): File Transfer success for  $file_name file on CDH.." >> $log_filename
        fi
        fi
        ssh ${userid}@${hs_cluster} /bin/bash << EOF
        rm -f $staging_dir/$application/archive/${actualfilename_pattern}*
        cd $staging_dir/$application/data/
        gunzip $file_name
        hadoop fs -conf ${adls_conf} -mkdir -p $target_dir/$tablename/date_part=$date_part
        hadoop fs -conf ${adls_conf} -put ${actualfilename_pattern}* $target_dir/$tablename/date_part=$date_part/
        gzip ${actualfilename_pattern}*
        mv $staging_dir/$application/data/$file_name $staging_dir/$application/archive/
EOF
        echo "INFO:$(date -d "today" +"%Y-%m-%d:%H:%M"): File Transfer success for  ${actualfilename_pattern}* file(s) on ADLS.." >> $log_filename
        ls -ltr --time-style=long-iso $file_name |  awk {'print $6,$7,$8'} > ${script_dir}/work/$application/${application}_${tablename}_last_file.check
done
fi
fi
}




#read input control file
################################(main)#################################
        if [ -e ${script_dir}/${application}.ctrl ]
        then
        source_dir=`sed 1d ${script_dir}/${application}.ctrl|head -1 | awk -F ',' {'print $3'}`
        consolidated_tranfer
        sed 1d ${script_dir}/${application}.ctrl|while read -r line
        do
         echo "INFO: Invoking file tranfer functions"
         parameter_parser
         file_transfer
        done
        else
                echo "File Transfer Failed , please check and verify all input parameters are correctly specified" | mailx -s " ${application} Failure Alert: FILE TRANSFER SCRIPT FAILED"   "praful.bigdata@gmail.com"
        fi

        final_status=$?
        if [ $final_status == 0 ];then
                echo "success"
                exit 0
        else
                echo "failure"
                echo "File Transfer Failed , please check and verify all input files are copied" | mailx -s " ${application} Failure Alert: FILE TRANSFER SCRIPT FAILED"   "praful.bigdata@gmail.com"
                exit 1
        fi
