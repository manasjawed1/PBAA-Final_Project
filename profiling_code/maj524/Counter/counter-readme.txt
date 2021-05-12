This file contains the instructions for running the simple line count program for both the intial datasets and the cleaned ones.

1) Transfer files using
scp CountRecs.java CountRecsMapper.java CountRecsReducer.java maj524@peel.hpc.nyu.edu:/home/maj524

2) Compile the files
javac -classpath `yarn classpath` -d . CountRecs.java CountRecsMapper.java CountRecsReducer.java 

3) Compile the counter jar
jar -cvf counter.jar *.class

4) Make directories for inputs, and transfer files
hdfs dfs -mkdir final
hdfs dfs -mkdir final/input

5) Put the data files in the input folder
hdfs dfs -put 18zpallagi.csv final/input
hdfs dfs -put RDC_Inventory_Core_Metrics_Zip_History.csv final/input

6) Get the cleaned files from the MapReduce Output
hdfs dfs -get final/real_cleaned/part-r-00000 real_cleaned.csv
hdfs dfs -get final/income_cleaned/part-r-00000 income_cleaned.csv

7) Transfer cleaned files
hdfs dfs -put real_cleaned.csv final/input
hdfs dfs -put income_cleaned.csv final/input

8) Run the line count program for all

hadoop jar counter.jar CountRecs final/input/RDC_Inventory_Core_Metrics_Zip_History.csv /user/maj524/final/real_profile_bfr_clean

hadoop jar counter.jar CountRecs final/input/18zpallagi.csv /user/maj524/final/income_profile_bfr_clean

hadoop jar counter.jar CountRecs final/input/real_cleaned.csv /user/maj524/final/real_profile_aftr_clean

hadoop jar counter.jar CountRecs final/input/income_cleaned.csv /user/maj524/final/income_profile_aftr_clean

9) Get results and see
hdfs dfs -get final/real_profile_bfr_clean/part-r-00000 real_profile_bfr_clean.csv
cat real_profile_bfr_clean.csv

hdfs dfs -get final/real_profile_aftr_clean/part-r-00000 real_profile_aftr_clean.csv
cat real_profile_aftr_clean.csv

hdfs dfs -get final/income_profile_bfr_clean/part-r-00000 income_profile_bfr_clean.csv
cat income_profile_bfr_clean.csv

hdfs dfs -get final/income_profile_aftr_clean/part-r-00000 income_profile_aftr_clean.csv
cat income_profile_aftr_clean.csv

