Instructions for the final project

Step 0 - Data Transfer
1) MapReduce Files and Data sent using 
scp [[file_names]] netid@peel.hpc.nyu.edu:/home/netid 

There are two datasets, and .java files for both are named the same in different folders.
They are transfered like this:
scp Clean.java CleanMapper.java CleanReducer.java netid@peel.hpc.nyu.edu:/home/netid 

Step 1 - Initial Profiling
1) Initial Profiling was done by using the line count program. Detailed profiling was hard to do at this stage as the data contained too many unimportant columns, and so I decided to clean and drop irrelevant columns before properly profiling.

Step 2 - Cleaning

The goal of the cleaning is to:
- Keep only the relevant columns
- Remove invalid zipcodes such as 0 and 99999 from the income file
- Remove entries that are not from the year 2018 from the real_estate file

1) Once the MapReduce files have been transferred, compile them like this:
javac -classpath `yarn classpath` -d . Clean.java CleanMapper.java CleanReducer.java

2) Then, compile them into jars:

-- for the income file: jar -cvf cleanIncome.jar \*.class 
-- for the real_estate file: jar -cvf cleanReal.jar \*.class

3) Make directories for inputs, and transfer files
hdfs dfs -mkdir final
hdfs dfs -mkdir final/input

4) Put the data files in the input folder
hdfs dfs -put 18zpallagi.csv final/input
hdfs dfs -put RDC_Inventory_Core_Metrics_Zip_History.csv final/input

5) Run the MapReduce jobs
hadoop jar cleanIncome.jar Clean final/input/18zpallagi.csv /user/netid/final/income_cleaned

hadoop jar cleanReal.jar Clean final/input/RDC_Inventory_Core_Metrics_Zip_History.csv /user/netid/final/real_cleaned

6) Get the cleaned files
hdfs dfs -get final/real_cleaned/part-r-00000 real_cleaned.csv
hdfs dfs -get final/income_cleaned/part-r-00000 income_cleaned.csv

7) Look at the difference between the original and the cleaned files
wc 18zpallagi.csv
wc income_cleaned.csv
head income_cleaned.csv
tail income_cleaned.csv

wc RDC_Inventory_Core_Metrics_Zip_History.csv
wc real_cleaned.csv
head real_cleaned.csv
tail real_cleaned.csv

Step 3 - Profiling + Preliminary Analysis in Impala
1) Make directory
hdfs dfs -mkdir final-Impala

2) Put files in new directory
hdfs dfs -put income_cleaned.csv final-Impala/
hdfs dfs -put real_cleaned.csv final-Impala/

3) Connect to impala
impala-shell
connect hc07.nyu.cluster;
use netid;

4) Load tables
DROP table if exists income_clean;
create table income_clean (zipcode INT, state STRING, agi_stub INT, n1 FLOAT, total_income FLOAT, state_and_local FLOAT, real_estate FLOAT, num_paid FLOAT, paid_amount FLOAT, taxable_amount STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;

load data inpath 'hdfs://horton.hpc.nyu.edu:8020/user/netid/final-Impala/income_cleaned.csv' overwrite into table income_clean; 

DROP table if exists real_estate_clean;

create table real_estate_clean (date_col INT, zipcode INT, median_listing_price FLOAT, active_listing_count FLOAT, median_per_sq_ft FLOAT, median_sq_ft STRING, avg_listing_price FLOAT, total_listing_count STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;

load data inpath 'hdfs://horton.hpc.nyu.edu:8020/user/netid/final-Impala/real_cleaned.csv' overwrite into table real_estate_clean; 

4.1) Do some profiling

-- For real_estate_clean
DESCRIBE real_estate_clean;

SELECT MIN(date_col), MAX(date_col) from real_estate_clean;

select min(median_listing_price), max(median_listing_price), min(active_listing_count), max(active_listing_count) from real_estate_clean;

select min(median_per_sq_ft), max(median_per_sq_ft), min(median_sq_ft), max(median_sq_ft) from real_estate_clean;

select min(avg_listing_price), max(avg_listing_price), min(total_listing_count), max(total_listing_count) from real_estate_clean;

-- For income_clean
DESCRIBE income_clean;

SELECT MIN(agi_stub), MAX(agi_stub), MIN(n1), MAX(n1) FROM income_clean;

SELECT MIN(total_income), MAX(total_income), MIN(state_and_local), MAX(state_and_local) FROM income_clean;

SELECT MIN(real_estate), MAX(real_estate), MIN(num_paid), MAX(num_paid) FROM income_clean;

SELECT MIN(paid_amount), MAX(paid_amount), MIN(taxable_amount), MAX(taxable_amount) FROM income_clean;

5) Perform descriptive analysis to understand the data
-- Code in ana_code

STEP 4 - Regression in Apache Spark
NOTE: There is a jupyter notebook called scratch_work that was used to come up with the code for spark. It presents information in a cleaner and more editable way.

The Goal: The goal of the regression in Apache Spark is to see if we can predict property prices, independent of state, depending on the average income. This is a simple analysis, but may correspond to the findings of affordability indexes, the same way the impala analysis did. 

Set up instructions:
module purge
module load python/gcc/3.7.9 
pyspark --deploy-mode client

Detailed code can be found in the spark-code.py file under ana_code/regression-apache-spark/spark-code.py 








