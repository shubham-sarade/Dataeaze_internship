from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace,year, to_date, when, expr
from pyspark.sql.types import DateType


spark = SparkSession.builder \
    .appName("Spark_Assignment") \
    .getOrCreate()

#By specifying the file path as file:///, you are indicating that the files are located on the local file system.
csv_df = spark.read.csv("file:///home/system/DZ/Spark/startup.csv", header=True)
parquet_df = spark.read.parquet("file:///home/system/DZ/Spark/consumerInternet.parquet")


combined_df = csv_df.union(parquet_df)

# combined_df.show(10)
# combined_df.printSchema()
# print (combined_df.count())
# combined_df.describe().show()

########################################...CLEANING...##################################################

cleaned_df = combined_df.drop("Remarks")
#cleaned_df.show()
cleaned_df = cleaned_df.withColumn("Amount_in_USD", regexp_replace(col("Amount_in_USD"), ",", ""))
cleaned_df = cleaned_df.withColumn("Amount_in_USD", regexp_replace(col("Amount_in_USD"), ",", ""))
cleaned_df = cleaned_df.withColumn("Amount_in_USD", when((col("Amount_in_USD") == 'N/A') | (col("Amount_in_USD") == ''), '0').otherwise(col("Amount_in_USD")))

cleaned_df = cleaned_df.withColumn("Amount_in_USD", when(col("Amount_in_USD").isNull(), '0').otherwise(col("Amount_in_USD")))
cleaned_df = cleaned_df.withColumn("Amount_in_USD", cleaned_df["Amount_in_USD"].cast("integer"))

#cleaned_df.show()

#If the input string does not match the specified format, the resulting value will be null. 
#to_date - for converting strings or timestamps to date columns. or formats Timestamp to Date
#cast - used to explicitly cast a column to a specific data type.

cleaned_df = cleaned_df.withColumn("Date", to_date(col("Date"), "dd/MM/yyyy").cast(DateType()))


cleaned_df = cleaned_df.withColumn("Year", year(col("Date"))).drop("Date")
cleaned_df = cleaned_df.withColumn("Year", expr("IF(LENGTH(Year) = 2, CONCAT('20', Year), Year)"))

#cleaned_df = cleaned_df.na.drop()
cleaned_df = cleaned_df.fillna(0)

# cleaned_df.write.format("csv").option("header", True).mode("overwrite").save("/home/system/DZ/spark.csv")


cleaned_df.show()
#cleaned_df.printSchema()
#print (cleaned_df.count())


cleaned_df.createOrReplaceTempView("startup")

#dictionary which contain all queries
queries = {
    1: "select count(Startup_Name) as NO_OF_STARTUP from startup where City= 'Pune' group by City",
    2: "select count(Startup_Name) as NO_OF_SEED_ANGEL_FUNDING_STARTUPS from startup where city= 'Pune' and InvestmentnType= 'Seed/ Angel Funding' group by city",
    3: "select sum(Amount_in_USD) as TOTAL_AMOUNT_RAISED_IN_PUNE_CITY from startup where city = 'Pune'",
    4: "select Industry_Vertical as TOP_5_INDUSTRY_VERTICAL_HAVING_HIGHEST_NO_OF_STARTUPS , count(Industry_Vertical) as NO_OF_STARTUPS from startup group by Industry_Vertical order by count(Startup_Name)desc limit 5",
    5: "select A.Investors_Name, A.Year, A.Amount_in_USD as Amount FROM startup A WHERE A.Amount_in_USD IN (SELECT MAX(B.Amount_in_USD) FROM startup B WHERE B.Year = A.Year GROUP BY B.Year)"
}

#execute each quary one by one & store in different folder.
i=0

for i in queries:
    q=queries[i]
    res=spark.sql(q)

    f = "file:///home/system/DZ/Spark/output/Q_"+str(i) 
    res.write.option("header", True)\
    .mode("overwrite")\
    .csv(f)