#! /bin/bash
#
############################################################################################
#                                         WD35S001 
#         
# Get data form the free api source and load the data to internal hive TABLE
#
# 1) source api : 
#			"https://vpic.nhtsa.dot.gov/api/vehicles/GetModelsForMake/*?format=csv"
#
# 2) Directory details:
#	1) Log directory			: /tmp/"$USER"/log/
#	2) Landing pad direcotry	: /tmp/"$USER"/landingpad/
#	3) HDFS directory			: /user/"$USER"/vehicle_data/
#
# 3) hive table is "vehicle_models"
#
# 4) Query & Analysis: Get top 5 brands with most models
#
#*******************************************************************************************
# Date    |   Description 
#-------------------------------------------------------------------------------------------
#26/04/25    Initial version            
#                            - Balaji Srinivasan    
############################################################################################
#                   
#--------------------------------------------------------------------------------------------
#                           Variable decalaration
#--------------------------------------------------------------------------------------------
# 
var_module_name="WD35S001"
var_source_api="https://vpic.nhtsa.dot.gov/api/vehicles/GetModelsForMake/*?format=csv"
var_log_loc="/tmp/"$USER"/log"
var_landpad_loc="/tmp/"$USER"/landingpad/"
var_hdfs_loc="/user/"$USER"/vehicle_data/"
var_date=$(date '+%Y-%m-%d')
var_time=$(date '+%H.%M.%S')

#--------------------------------------------------------------------------------------------
#                           Functions decalaration
#--------------------------------------------------------------------------------------------

# Print the module info
print_module_info(){
	echo ""
	echo "#############################################"
	echo "                 "$var_module_name
	echo "#############################################"
	echo ""
}

#Print the Time stamps info
print_time_stamps(){
var_start_end=$1
	if [ $var_start_end = "start" ]
	then
		echo ""
		echo "start date & time: $var_date & $var_time"
	else
		echo ""
		echo "end date & time  : $var_date & $var_time"
		echo "#############################################"
	fi
}

#Script Abnormal termination
exit_script(){
	echo ""
	echo "----------> program terminated abnormal <-----------"
	print_time_stamps "end"
}

#Ensure the require directory are available if not create it
#	1) Log directory			: /tmp/"$USER"/log/
#	2) Landing pad direcotry	: /tmp/"$USER"/landingpad/
#	3) HDFS directory			: /user/"$USER"/vehicle_data/
#
create_required_directories(){
	if [ -d $var_log_loc ]
	then
		echo " "
		echo "Log file location, where we create our log details"
		echo $var_time" Log file location, where we create our log details" >> $var_log_loc/$var_module_name${var_date}_${var_time}.log
	else
		mkdir $var_log_loc
		if [ $? != 0]
		then
		   exit_script
		   exit 11
		fi
		echo " "
		echo "Log file location is absent, hence created"
		echo $var_time" Log file location is absent, hence created" >> $var_log_loc/$var_module_name${var_date}_${var_time}.log
	fi

	if [ -d $var_landpad_loc ]
	then
		echo " "
		echo "Landing Pad, (where I am going to receive (landing pad) is present"
		echo $var_time "Landing Pad, (where I am going to receive (landing pad) and validate & clean the data (staging)) is present" >> $var_log_loc/$var_module_name${var_date}_${var_time}.log
	else
		mkdir $var_landpad_loc
		if [ $? != 0]
		then
		   exit_script
		   exit 12
		fi
		chmod -R 770 $var_landpad_loc
		echo " "
		echo "Landing Pad is not present, hence created"
		echo $var_time "Landing Pad is not present, hence created" >> $var_log_loc/$var_module_name${var_date}_${var_time}.log
	fi

	echo "Check whether the above (hdfs datalake) dir is created in Hadoop"
	hadoop fs -test -d $var_hdfs_loc
	if [ $? -eq 0 ]
	then
	  echo $var_time "hadoop data lake directory is exists/created " >> $var_log_loc/$var_module_name${var_date}_${var_time}.log
	else  
	  echo $var_time "hadoop directory is not exists/ failed to create the hadoop directory "$var_hdfs_loc >> $var_log_loc/$var_module_name${var_date}_${var_time}.log
	  hadoop fs -mkdir $var_hdfs_loc
		if [ $? != 0]
		then
		   exit_script
		   exit 13
		fi
	fi

}

# Call below REST API 
#	https://vpic.nhtsa.dot.gov/api/vehicles/GetModelsForMake/honda?format=csv
#   download the csv file to landing pad bath
#   remove the header from the csv file before loading to the hive tables

get_file_from_api(){
	var_api_downloaded_data="vechiledata_"${var_date}_${var_time}".csv"
    var_landpad_data="${var_landpad_loc}${var_api_downloaded_data}"
	echo $var_landpad_data
	var_HTTP_Response=$(wget --server-response -O $var_landpad_data $var_source_api 2>&1 | grep "HTTP/")
	echo " "
	echo $var_time "REST API is called for pulling the file-"$var_source_api >> $var_log_loc/$var_module_name${var_date}_${var_time}.log
	echo " "
	echo "File downloaded operation HTTP response --> "$var_HTTP_Response
	echo $var_time "File downloaded operation HTTP response --> "$var_HTTP_Response >> $var_log_loc/$var_module_name${var_date}_${var_time}.log
	
	if (grep -q '200' <<< "$var_HTTP_Response")
	then
	    echo " "
		echo $var_landpad_data "File is downloaded from the RESP API" 
		echo $var_time "{$var_landpad_data} File is downloaded from the RESP API" >> $var_log_loc/$var_module_name${var_date}_${var_time}.log

# Clean the data 
#	1) remove the csv header before loading
		sed -i '1d' $var_landpad_data
		if [ $? != 0 ] 
		then 
			exit_script
			exit 14
		else
			echo " "
			echo "Remove csv header information"
			echo $var_time "Remove csv header information" >> $var_log_loc/$var_module_name${var_date}_${var_time}.log
		fi
		
#push data to hdfs
		hadoop fs -copyFromLocal ${var_landpad_data} ${var_hdfs_loc}
		if [ $? != 0 ]
		then
			exit_script
			exit 15
		else
			echo "data is copied to HDFS to location" ${var_hdfs_loc}
			echo $var_time "data is copied to HDFS to location" ${var_hdfs_loc} >> $var_log_loc/$var_module_name${var_date}_${var_time}.log
		fi

#create touch files
		var_touchfile=${var_hdfs_loc}"_SUCCESS"
		hadoop fs -touchz ${var_touchfile}
	else 
		exit_script
		exit 17
		
	fi
}

# Clean the data 
#	2) Load only the data greater then in id column
#   3) also remove data from landing pad 

data_cleaning_process(){	
	var_no_rows=$(awk -F',' '{if($1 >= "0") print $1,$2,$3,$4;}' $var_landpad_data | wc -l)	
	if [ $var_no_rows = "0" ]
	then
		echo " "
		echo "====> No Warning record extracted! <======"
		echo "====> No Warning record extracted! <======" >> $var_log_loc/$var_module_name${var_date}_${var_time}.log
		exit_script
		exit 16
	else
		echo " "
		echo $var_no_rows "Number of Warning record extracted" 
		echo $var_time $var_no_rows "Number of Warning record extracted" >> $var_log_loc/$var_module_name${var_date}_${var_time}.log
	fi
	
#remove data form landing pad
	rm -r $var_landpad_data
	if [ $? != 0 ]
	then
		exit_script
		exit 16
	else
	    echo " "
		echo "landing pad is archieved" 
		echo $var_time "landing pad is archieved" >> $var_log_loc/$var_module_name${var_date}_${var_time}.log
	fi
}

# create hive tables vechile
#		- since there is possiblity we will have row with comma so below syntax is used to avoid it
#
#				ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
#				WITH SERDEPROPERTIES (
#				  'separatorChar' = ',')

create_hivetbl(){
hive -e "CREATE TABLE IF NOT EXISTS vehicle_models(
    Make_ID INT,
    Make_Name STRING,
    Model_ID INT,
    Model_Name STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'separatorChar' = ','
)
STORED AS TEXTFILE;"

if [ $? != 0 ] 
then 
	echo " "
	echo "Hive table(vehicle_models) creation failed"
	echo $var_time "Hive table(vehicle_models) creation failed" >> $var_log_loc/$var_module_name${var_date}_${var_time}.log
	exit_script
	exit 17
else
	echo " "
	echo "Hive table(vehicle_models) created successfully"
	echo $var_time "Hive table(vehicle_models) created successfully" >> $var_log_loc/$var_module_name${var_date}_${var_time}.log
fi
}

# load  hive tables vechile
#

load_hivetbl(){
var_srcfile=${var_hdfs_loc}${var_api_downloaded_data}
hive -e "load data  inpath '$var_srcfile' OVERWRITE INTO TABLE vehicle_models;"
if [ $? != 0 ] 
then 
	echo " "
	echo "Hive table(vehicle_models) data load failed"
	echo $var_time "Hive table(vehicle_models) data load failed" >> $var_log_loc/$var_module_name${var_date}_${var_time}.log
	exit_script
	exit 18
else
	echo " "
	echo "Hive table(vehicle_models) data loaded successfully"
	echo $var_time "Hive table(vehicle_models) data loaded successfully" >> $var_log_loc/$var_module_name${var_date}_${var_time}.log
fi
}

# query hive table 
#			Get top 5 brands with most models
#

hive_query()
{

echo "*******************************************************************"
echo "                 Hive Query & Analysis                             "
echo "              Get top 5 brands with most models                    "
echo "*******************************************************************"

hive -e "SELECT Make_Name, COUNT(DISTINCT Model_ID) AS model_count
FROM vehicle_models
GROUP BY Make_Name
ORDER BY model_count DESC
LIMIT 5;"

if [ $? != 0 ] 
then 
	echo " "
	echo "Hive Query top 5 brands with models failed"
	echo $var_time "Hive Query top 5 brands with models failed" >> $var_log_loc/$var_module_name${var_date}_${var_time}.log
	exit_script
	exit 19
else
	echo " "
	echo "Hive Query top 5 brands with models ran"
	echo $var_time "Hive Query top 5 brands with models ran" >> $var_log_loc/$var_module_name${var_date}_${var_time}.log
fi
}

#---------------------------------------------------------------------------------------------------------
#1. start the process 
#   print the Module info with time stamp
print_module_info 
print_time_stamps "start"
#---------------------------------------------------------------------------------------------------------

#---------------------------------------------------------------------------------------------------------
#2. create required directories
#---------------------------------------------------------------------------------------------------------
create_required_directories

#---------------------------------------------------------------------------------------------------------
#3. download a file from RESTAPI
#---------------------------------------------------------------------------------------------------------
get_file_from_api

#---------------------------------------------------------------------------------------------------------
#3. Clean the data before loading
#---------------------------------------------------------------------------------------------------------
data_cleaning_process

#---------------------------------------------------------------------------------------------------------
#4. create hive table
#---------------------------------------------------------------------------------------------------------
create_hivetbl

#---------------------------------------------------------------------------------------------------------
#5. load data to hive table
#---------------------------------------------------------------------------------------------------------
load_hivetbl

#---------------------------------------------------------------------------------------------------------
#6. Query & Analysis
#---------------------------------------------------------------------------------------------------------
hive_query

#---------------------------------------------------------------------------------------------------------
#7. program termination
#---------------------------------------------------------------------------------------------------------
print_time_stamps "end"