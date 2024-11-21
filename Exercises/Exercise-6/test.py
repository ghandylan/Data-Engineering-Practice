from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("Exercise6").enableHiveSupport().getOrCreate()
df = spark.read.format("csv").option("header", "true").load("data\\Divvy_Trips_2019_Q4.zip")
