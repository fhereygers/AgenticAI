import os
import xml.etree.ElementTree as ET

from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import *


if os.path.exists("/etc/hadoop/conf/hive-site.xml"):
        tree = ET.parse("/etc/hadoop/conf/hive-site.xml")
        root = tree.getroot()
        for prop in root.findall("property"):
            if prop.find("name").text == "hive.metastore.warehouse.dir":
                # catch erroneous pvc external storage locale
                if len(prop.find("value").text.split("/")) > 5:
                    STORAGE = (
                        prop.find("value").text.split("/")[0]
                        + "//"
                        + prop.find("value").text.split("/")[2]
                    )


hive_database= "onlinebanking"

STORAGE = STORAGE + "/data/" + hive_database

print(STORAGE)


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






# spark.sql("show databases").show()



# 
# clean up
#
# hive_table = hive_database + "." + "customer_profiles"
# spark.sql("drop table " + hive_database + "." + "customer_profiles").show()
# spark.sql("drop table " +  hive_database + "." + "product_offers").show()
# spark.sql("drop table " +  hive_database + "." + "transaction_data").show()
# spark.sql("drop database " + hive_database).show()

