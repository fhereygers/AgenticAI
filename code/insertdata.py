from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import *


STORAGE = "s3a://goes-se-sandbox/data/onlinebankingfh"
hive_database= "onlinebankingfh"

!hdfs dfs -mkdir {STORAGE}

!hdfs dfs -ls {STORAGE}

!hdfs dfs -copyFromLocal /home/cdsw/data/customer_profiles.csv {STORAGE}/customer_profiles.csv
!hdfs dfs -copyFromLocal /home/cdsw/data/product_offers.csv {STORAGE}/product_offers.csv
!hdfs dfs -copyFromLocal /home/cdsw/data/transaction_data.csv {STORAGE}/transaction_data.csv

!hdfs dfs -ls {STORAGE}

spark = SparkSession.builder.appName("PythonSQL").master("local[*]").getOrCreate()

spark.sql("create database "+ hive_database).show()


# 
# Create customers profiles hive table
#


path = f"{STORAGE}/customer_profiles.csv"
hive_table = hive_database + "." + "customer_profiles"

cust_data = spark.read.load( path , format="csv", inferSchema="true", header="true")

cust_data.show()

cust_data.printSchema()

cust_data.write.format("parquet").mode("overwrite").saveAsTable(hive_table)

# 
# Create product offers hive table
#


path = f"{STORAGE}/product_offers.csv"
hive_table = hive_database + "." + "product_offers"

cust_data = spark.read.load( path , format="csv", inferSchema="true", header="true")

cust_data.show()

cust_data.printSchema()

cust_data.write.format("parquet").mode("overwrite").saveAsTable(hive_table)

# 
# Create transaction data hive table
#


path = f"{STORAGE}/transaction_data.csv"
hive_table = hive_database + "." + "transaction_data"

cust_data = spark.read.load( path , format="csv", inferSchema="true", header="true")

cust_data.show()

cust_data.printSchema()

cust_data.write.format("parquet").mode("overwrite").saveAsTable(hive_table)






spark.sql("show databases").show()

spark.sql("create database onlinebankingfh").show()

spark.sql("drop database onlinebankingfh").show()

spark.sql("create database "+ hive_database).show()

spark.sql("show tables in " + hive_database).show()

spark.sql("select * from " + hive_table).show()

spark.sql("select count(*) from " + hive_table).show()


# 
# clean up
#
# hive_table = hive_database + "." + "customer_profiles"
# spark.sql("drop table " + hive_database + "." + "customer_profiles").show()
# spark.sql("drop table " +  hive_database + "." + "product_offers").show()
# spark.sql("drop table " +  hive_database + "." + "transaction_data").show()
# spark.sql("drop database " + hive_database).show()

