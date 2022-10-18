# hadoop-bigdata
This assignment took restaurant data from the yelp-academic-dataset and filtered it, using pyspark, down to four star restaurants, in philadelphia, opening at 8am. It then used the tom-tom api to calculate the best routes for 5 trucks to deliver to these restaurants from the airport

In order to run these scripts, the following are required: 

Linux OS with: Hadoop and Pyspark installed. 
(You could do this with windows but i do not suggest attempting to install hadoop on windows.)

Please refer to the following links to install hadoop and Pyspark 
https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html

$ pip install pyspark
or 
https://spark.apache.org/docs/latest/api/python/getting_started/install.html

$ pip install pandas

From there, please download the yelp dataset from 
https://www.yelp.com/dataset

and follow this guide to convert the dataset from json to csv 
https://github.com/Yelp/dataset-examples

from there copy the yelp_business_dataset.csv into the hadoop filesystem

$ hdfs dfs -mkdir /user/data
$ HADOOP_HOME/bin/hadoop fs -put <location of csv file> <location of hdfs>

Please ensure that sync_batch.json and waypoin.json are within the same directory as the individual_3759233.py script.
You should now be able to run the individual_3759233.py script, without any issues

![alt text](https://i.imgur.com/PhKDUaL.jpg)
