from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, StringType, lower, regexp_replace,expr
from pyspark.sql.types import DateType

# spark = SparkSession.builder \
#     .appName("Spark_Assignment") \
#     .getOrCreate()


spark = SparkSession.builder \
    .appName("Spark_Assignment") \
    .config("spark.jars", "/home/system/Downloads/mysql-connector-java-8.0.28.jar") \
    .getOrCreate()


csv_df = spark.read.csv("file:///home/system/DZ/Spark/startup.csv", header=True)
parquet_df = spark.read.parquet("file:///home/system/DZ/Spark/consumerInternet.parquet")

combined_df = csv_df.union(parquet_df)

########################################...CLEANING...##################################################

#trim function is used to remove any spaces from the start or end of the strings.
#regexp_replace - any spaces within the string will remain unchanged.

combined_df = combined_df.withColumn("startup_name_processed", trim(regexp_replace(col("Startup_Name"), " ", "")))


#lower function - mkae 1st word in lower case.
combined_df = combined_df.withColumn("City", lower(col("City")))



combined_df.write \
  .format("jdbc") \
  .option("driver","com.mysql.cj.jdbc.Driver") \
  .option("url", "jdbc:mysql://localhost:3306/dz") \
  .option("dbtable", "startup1") \
  .mode("overwrite")\
  .option("user", "root") \
  .option("password", "cdac123") \
  .save()


