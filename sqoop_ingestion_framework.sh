#!/bin/bash
##########################################################################################################################################################################################################
#  Script Name:   sqoop_ingestion.sh                                                                                                                                    #
#  Author Name :   Praful Kumar Bhise<praful.bigdata@gmail.com>                                                                                                                #                                                                                                                                                                                                                                                    #
#  Description: creates sqoop incremental job and snapshot statement in order to import data from RDBMS systems into ADLS                                                                               #
#=========================================================================================================================                                              #                                            #
##########################################################################################################################################################################################################
application=$1

export PATH=$PATH:`dirname $0`
export HOME='/home/datalake_script'
export LOGON='/home/datalake_script/dl_app_specific_logon'
export date_part=`date -d "today" +"%Y-%m-%d"`

if [ -e $HOME/global_env.var ];then
. $HOME/global_env.var
else
        echo "Sqoop Import Failed,Unable to import global variable file.  please check and verify all input parameters are correctly specified before executing sqoop ingestion script " | mailx -s "Failure Alert: SQOOP INGESTION SCRIPT FAILED"   "praful.bigdata@gmail.com"
        exit 1
fi

if [ -e $LOGON/${application}.logon ];then
. $LOGON/${application}.logon
else
        echo "Sqoop Import Failed,Unable to application logon file.  please and verify all input parameters are correctly specified while before sqoop ingestion script" | mailx -s "Failure Alert: SQOOP INGESTION SCRIPT FAILED"   "praful.bigdata@gmail.com"
        exit 1
fi


export APPLICATION_DIR="/prod/common/datalake_script/adls/sqoop_script"
sqoop_dir="${APPLICATION_DIR}/sqoop_java_file"
#Read a file in a array
#########################################################################################
parameter_parser()
{
        IFS=',' read -a array <<< $line
        table_type=${array[0]}
        db_name=${array[1]}
        hive_normal=${array[2]}
        rdbms_table_name=${array[3]}
        schema_name=${array[4]}
        hive_table_name=${array[5]}
        target_dir=${array[6]}
        where_clause=${array[7]}
        split_by_col=${array[8]}
        fetch_size=${array[9]}
        num_mapper=${array[10]}
        check_column=${array[11]}
        last_value=${array[12]}


log_filename="${APPLICATION_DIR}/logs/${application}_${schema_name}_${rdbms_table_name}_$(date -d "today" +"%Y%m%d%H%M").log"
        echo "#################SQOOP IMPORT INVOACATION########################" >> $log_filename
        echo "INFO:$(date -d "today" +"%Y-%m-%d:%H:%M"): Initiating sqoop import job" >> $log_filename
        echo "INFO:$(date -d "today" +"%Y-%m-%d:%H:%M"): Reading a control file in an array" >> $log_filename
        echo "INFO:$(date -d "today" +"%Y-%m-%d:%H:%M"): Sqoop import is initiated for a table - ${application}_$rdbms_table_name" >> $log_filename


}

#Set up DB connection URL
##############################################################################################
db_connector()
{
        echo "INFO:$(date -d "today" +"%Y-%m-%d:%H:%M"): Setting connection DB URL" >> $log_filename
        if [ $db_name == 'teradata' ]; then
                db_connection_URL="jdbc:teradata://${td_server_name}/Database="${schema_name}" --username "${td_username}" --password "${td_password}""
        echo "INFO:$(date -d "today" +"%Y-%m-%d:%H:%M"): Connection DB URL set up successfully for Teradata" >> $log_filename
        elif [ $db_name == 'oracle' ]; then
                db_connection_URL="jdbc:oracle:thin:${ora_username}/${ora_password}@${ora_server_name}:${ora_port}/${ora_sid}"
                echo "INFO:$(date -d "today" +"%Y-%m-%d:%H:%M"): Connection DB URL set up successfully for Oracle" >> $log_filename
        elif [ $db_name == 'sqlserver' ]; then
                echo -n "${sql_password}" > $LOGON/password/${application}_pwd.password
                hadoop fs -put -f $LOGON/password/${application}_pwd.password $SQOOP_STAGE/password/
                db_connection_URL="jdbc:sqlserver://${sql_server_name}:${sql_server_port};databaseName="${schema_name}" --username "${sql_username}" --password-file $SQOOP_STAGE/password/${application}_pwd.password"
                echo "INFO:$(date -d "today" +"%Y-%m-%d:%H:%M"): Connection DB URL set up for successfully Sql Server" >> $log_filename
        else
                echo "ERROR:$(date -d "today" +"%Y-%m-%d:%H:%M"): Please Specify Valid Database name to connect to!" >> $log_filename
                echo "Sqoop Import Failed for a table - $rdbms_table_name, Please Specify Valid Database name to connect to!" | mailx -s "Failure Alert: SQOOP FAILED for a table $rdbms_table_name" -a "$log_filename" "praful.bigdata@gmail.com"
                exit 1
        fi
}

#Assign variables with respect to parameters
#########################################################################################
parameter_builder()
{
        if [ -z "$rdbms_table_name" ]; then
                echo "ERROR:$(date -d "today" +"%Y-%m-%d:%H:%M"): Please provide relational table name!" >> $log_filename
                exit 1
        else
                if [ $db_name == 'oracle' ];then
                rdbms_table=--table" ""${schema_name}.${rdbms_table_name}"
                else
                rdbms_table=--table" ""${rdbms_table_name}"
                fi
        fi
        if [ $hive_normal == 'hive' ]; then
                hive_import=--hive-import
                hive_table=--hive-table" ""${hive_table_name}"
        elif [ $hive_normal == 'normal' ]; then
                hive_import=''
        else
                echo "ERROR:$(date -d "today" +"%Y-%m-%d:%H:%M"): Please specify whether it is normal or hive import!" >> $log_filename
                echo "Sqoop Import Failed for a table - $rdbms_table_name, Please specify whether it is normal or hive import!" | mailx -s "Failure Alert: SQOOP FAILED for a table $rdbms_table_name" -a "$log_filename" "praful.bigdata@gmail.com"
                exit 1
        fi
        if [ -z "$target_dir" ]; then
                target_directory=''
        else
                target_directory=--target-dir" ""${target_dir}/date_part=${date_part}"
        fi
        if [ -z "$num_mapper" ]; then
                num_mappers=''
        else
                num_mappers=--num-mappers" ""${num_mapper}"
        fi
        if [ -z "$split_by_col" ]; then
                splitby=''
        else
                splitby=--split-by" ""${split_by_col}"
        fi
        if [ -z "$where_clause" ]; then
                where_condition=''
        else
                where_condition=--where" ""${where_clause}"
        fi
        if [ -z "$fetch_size" ]; then
                fetch_sz=''
        else
                fetch_sz=--fetch-size" ""${fetch_size}"
        fi
        if [ -z "$check_column" ]; then
                check_column=''
        else
                check_column=--check-column" ""${check_column}"
        fi
        if [ -z "$last_value" ]; then
                last_value=''
        else
                last_value=--last-value" ""${last_value}"
        fi

        echo "INFO:$(date -d "today" +"%Y-%m-%d:%H:%M"): All parameters are assigned the values " >> $log_filename
}
#Sqoop increament import logic
#################################################################################
incremental_import()
{
         sqoop job --exec ${application}_$rdbms_table_name &>> $log_filename
                sqoop_error=$?
                if [ $sqoop_error == 0 ]
         then
                fl_name=`hadoop fs -ls $SQOOP_STAGE/data/${application}_$rdbms_table_name/part* |tail -1 | awk '{print $NF}'`
                check_fl=`hadoop fs -ls $fl_name | awk '{print $5}'`
                if [ $check_fl != 0 ];then
                        hadoop fs -conf $ADLS_CONF -mkdir -p $target_dir/date_part=${date_part}
                        hadoop fs -conf $ADLS_CONF -cp $fl_name $target_dir/date_part=${date_part}/
                        echo "INFO:$(date -d "today" +"%Y-%m-%d:%H:%M"): Sqoop import completed successfully! Please verify the data in target ADLS system" >> $log_filename
#                       echo "Sqoop Import success for a table - $rdbms_table_name" | mailx -s "${application} - Success Alert: SQOOP SUCCESS for a table $rdbms_table_name" "praful.bigdata@gmail.com"

                else
                        echo "INFO:$(date -d "today" +"%Y-%m-%d:%H:%M"): Sqoop import completed successfully! No new records found in source system, Data in Target ADLS system remains intact" >> $log_filename
#                       echo "Sqoop Import success for a table - $rdbms_table_name" | mailx -s "${application} - Success Alert: SQOOP SUCCESS for a table $rdbms_table_name" "praful.bigdata@gmail.com"
                fi
        else
                echo "ERROR:$(date -d "today" +"%Y-%m-%d:%H:%M"): Failure occured while importing the data, Please investigate root cause before re-run" >> $log_filename
                echo "Sqoop Import Failed for a table - $rdbms_table_name, please find attached log for more details" | mailx -s "Failure Alert: SQOOP IMPORT FAILED for a table $rdbms_table_name" -a "$log_filename" "praful.bigdata@gmail.com"
                fi
}
#Sqoop Snapshot import logic
##################################################################################
snapshot_import()
{

        hadoop fs -conf $ADLS_CONF -rm -R -skipTrash $target_dir
        check_adls_fl=`hadoop fs -conf $ADLS_CONF -ls $target_dir |wc -l`
        if [ $check_adls_fl == 0 ];then
                echo "INFO:$(date -d "today" +"%Y-%m-%d:%H:%M"): Deleted Target alds directory..starting snapshot import process for a table - $rdbms_table_name" >> $log_filename
        hive_char=`printf "\u0007"`
        sqoop import -conf $ADLS_CONF \
        --connect ${db_connection_URL} \
        $rdbms_table \
        --fields-terminated-by $hive_char \
        --null-string '\\N' \
        --null-non-string '\\N' \
        --outdir $sqoop_dir \
        $target_directory \
        $where_condition \
        $num_mappers \
        $fetch_sz \
        $splitby \
        --verbose &>> $log_filename
#       sqoop_error=1
        sqoop_error=$?
                if [ $sqoop_error == 0 ];then
                echo "INFO:$(date -d "today" +"%Y:%m:%d:%H:%M"): Snapshot Data imported successfuly for a table - $rdbms_table_name , please verify data in ADLS." >> $log_filename
        echo "Sqoop Import success for a table - $rdbms_table_name" | mailx -s "${application} - Success Alert: SQOOP SUCCESS for a table $rdbms_table_name" "praful.bigdata@gmail.com"
        else
 #               echo "ERROR:$(date -d "today" +"%Y:%m:%d:%H:%M"): Error occured while importing data for a table - $rdbms_table_name, Please investigate root cause before re-run " >> $log_filename
                echo "Sqoop Import Failed for a table - $rdbms_table_name, please find attached log for more details" | mailx -s "Failure Alert: SQOOP FAILED for a table $rdbms_table_name" -a "$log_filename" "praful.bigdata@gmail.com"
                fi

        else
                echo "ERROR:$(date -d "today" +"%Y:%m:%d:%H:%M"): Error deleting the files from ADLS - $rdbms_table_name, please investigate root cause before re-run" >> $log_filename
#               echo "Sqoop Import Failed for a table - $rdbms_table_name, please find attached log for more details" | mailx -s "Failure Alert: SQOOP FAILED for a table $rdbms_table_name" -a "$log_filename" "praful.bigdata@gmail.com"
        fi
}
#main sqoop import function
#############################################################################
import_func()
{
        if [[ $table_type == 'append' ]] || [[ $table_type == 'lastmodified' ]]; then
        check_status=`sqoop job --list | grep ${application}_$rdbms_table_name |wc -l`
        if [ $check_status == 0 ];then
        echo "Assigning increment columns"
        if [ $table_type == 'append' ];then
        append_col='--incremental'
        else
        append_col='--append --incremental'
        fi
        hive_char=`printf "\u0007"`
        sqoop job -conf $ADLS_CONF \
        --create ${application}_$rdbms_table_name -- import \
        --connect ${db_connection_URL} \
        $rdbms_table \
        --fields-terminated-by $hive_char \
        --null-string '\\N' \
        --null-non-string '\\N' \
        --outdir $sqoop_dir \
        --target-dir $SQOOP_STAGE/data/${application}_$rdbms_table_name \
        $where_condition \
        $num_mappers \
        $fetch_sz \
        $splitby \
        $append_col $table_type \
        $check_column \
        $last_value \
        --verbose

        sqoop_error=$?
                if [ $sqoop_error == 0 ];then
                        echo "INFO:$(date -d "today" +"%Y:%m:%d:%H:%M"): New job is created, Invoking a sqoop increamental job.." >> $log_filename
                        incremental_import
                fi
        else
                        echo "INFO:$(date -d "today" +"%Y:%m:%d:%H:%M"): Invoking existing Sqoop Job ! If you wish to create new sqoop job please delete the existing job using below syntax" >> $log_filename
                        echo "INFO:$(date -d "today" +"%Y:%m:%d:%H:%M"): syntax and command to delete sqoop job : sqoop job --delete ${application}_$rdbms_table_name " >> $log_filename
                        incremental_import
        fi
        else
                        echo "INFO:$(date -d "today" +"%Y:%m:%d:%H:%M"): Initiating snapshot sqoop import process" >> $log_filename
                        snapshot_import
        fi

}
#read input control file
################################(main)#################################
        if [ -e ${APPLICATION_DIR}/${application}.ctrl ]
        then
        num_of_tables=`cat ${APPLICATION_DIR}/${application}.ctrl | sed 1d | wc -l`
        sed 1d ${APPLICATION_DIR}/${application}.ctrl|while read -r line
        do
                parameter_parser
                db_connector
                parameter_builder
                import_func
        done
        else
                echo "Sqoop Import Failed , please check and verify all input parameters are correctly specified" | mailx -s "Failure Alert: SQOOP INGESTION SCRIPT FAILED"   "praful.bigdata@gmail.com"
        fi

        final_status=$?
        if [ $final_status == 0 ];then
                echo "success"
        echo "UTM-APEX SQOOP IMPORT job Iteration completed" | mailx -s "Final UTM-APEX Success Alert: UTM-APEX - SQOOP INGESTION SCRIPT SUCCESS"   "praful.bigdata@gmail.com"
                exit 0
        else
                echo "failure"
                exit 1
        fi
#################################(END OF SCRIPT)######################################
