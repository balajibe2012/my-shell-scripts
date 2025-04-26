Description: 	This use case is about receiving data from the RESTAPI and clean load copy the data to HDFS and then loading into HIVE tables. Vehicle Model Analytics Using Hive & Public API 
Actual script:	https://github.com/balajibe2012/my-shell-scripts
Source: 	 I have used United States Department of Transportation free RESTAPI for data as below,
	
	
	https://vpic.nhtsa.dot.gov/api/
	https://vpic.nhtsa.dot.gov/api/vehicles/GetModelsForMake/*?format=csv
	
	
	Below  sample CSV file will be downloaded when we hit the Free API.
	
Constraint:	The "make_name" column will have comma in the data.
Hive table structure 	make_id
	make_name
	model_id
	model_name
	Integer
	String
	Integer
	string
Script standards:	General formation
	comments
	Program name 
	WD35S001
	(In Linux) /tmp/<user>/log/
	Store log files with naming convention. File Format - "<programname>_<date>_time.log"
	(In Linux) /tmp/<user>/landingpad/
	Linux Landing pad location (will be deleted once the process is completed)
	File Format -vechiledata_2025-04-26_15.50.58.csv 
	(HDFS)  /user/<user>/vehicle_data/
	HDFS location
File Format - vechiledata_2025-04-26_15.50.58.csv 
Touch file format- _SUCCESS
Hive Query used	1) Table creation
	hive -e "CREATE TABLE IF NOT EXISTS ModelsForMake
	(make_id Int,
	mfr_name String,
	model_id Int
	make_name String
	)
	ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
	WITH SERDEPROPERTIES (
	  'separatorChar' = ','
	)
	STORED AS TEXTFILE;
	My learning
	ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
	WITH SERDEPROPERTIES (
	  'separatorChar' = ','
	
	We need to include this because, this will handle comma separated file + column data having any comma. The load will not be a problem
	        
	2) Load query
	hive -e "load data local inpath '$var_srcfile' OVERWRITE INTO TABLE ModelsForMake;"
	3) Analysis query 
	Get top 5 brands with most models
	SELECT Make_Name, COUNT(DISTINCT Model_ID) AS model_count
	FROM vehicle_models
	GROUP BY Make_Name
	ORDER BY model_count DESC
	LIMIT 5;
Unit testing	Before look of Hive
	
	
	Script execution
	
	
	
	
	
	
	
	
	
	
	After look of Hive:
	        1) Table created 
	        Tables created
	
	        2)  Verify the table count
	
	
	
	(Note: both count are same, so the table loaded success) 
	
	Sample select query
	
![image](https://github.com/user-attachments/assets/7b1d22a4-9fb7-4492-bc6d-f643969e77c0)

