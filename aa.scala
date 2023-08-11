import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType


object aa {
  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("ScalaAssignment").config("spark.master", "local")
      .getOrCreate()

    print(spark)

    val csvPath = "/home/system/DZ/Scala/startup.csv"
    val csvDF = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(csvPath)

    val parquetPath = "/home/system/DZ/Scala/consumerInternet.parquet"
    val parquetDF = spark.read.parquet(parquetPath)

    val combinedDF = csvDF.union(parquetDF)

    ////////////////////...CLEANING...//////////////////////////////////////

    val modifiedDF = combinedDF.drop("Remarks")


    val modifiedDF1 = modifiedDF.withColumn("amount_in_usd", regexp_replace(col("Amount_in_USD"), ",", ""))
    val modifiedDF2 = modifiedDF1.withColumn("Amount_in_USD", when((col("Amount_in_USD") === "N/A") || (col("Amount_in_USD") === ""), "0").otherwise(col("Amount_in_USD")))
    val modifiedDF3 = modifiedDF2.withColumn("Amount_in_USD", when(col("Amount_in_USD").isNull, "0").otherwise(col("Amount_in_USD")))
    val modifiedDF4 = modifiedDF3.withColumn("Amount_in_USD", col("Amount_in_USD").cast("integer"))

    //If the input string does not match the specified format, the resulting value will be null.
    //to_date - for converting strings or timestamps to date columns. or formats Timestamp to Date
    //cast - used to explicitly cast a column to a specific data type.

    val modifiedDF5 = modifiedDF4.withColumn("Date", to_date(col("Date"), "dd/MM/yyyy").cast(DateType))
    val modifiedDF6 = modifiedDF5.withColumn("Year", year(col("Date"))).drop("Date")
    val modifiedDF7 = modifiedDF6.withColumn("Year", expr("IF(length(Year) = 2, concat('20', Year), Year)"))


    val filledDF = modifiedDF7.na.fill(0)


    // Create or replace a temporary view for the DataFrame
    filledDF.createOrReplaceTempView("startup")

    // Define the queries
    val queries = Map(
      1 -> "select count(Startup_Name) as NO_OF_STARTUP from startup where City = 'Pune' group by City",
      2 -> "select count(Startup_Name) as NO_OF_SEED_ANGEL_FUNDING_STARTUPS from startup where city = 'Pune' and InvestmentnType = 'Seed/ Angel Funding' group by city",
      3 -> "select sum(Amount_in_USD) as TOTAL_AMOUNT_RAISED_IN_PUNE_CITY from startup where city = 'Pune'",
      4 -> "select Industry_Vertical as TOP_5_INDUSTRY_VERTICAL_HAVING_HIGHEST_NO_OF_STARTUPS, count(Industry_Vertical) as NO_OF_STARTUPS from startup group by Industry_Vertical order by count(Startup_Name) desc limit 5",
      5 -> "select A.Investors_Name, A.Year, A.Amount_in_USD as Amount FROM startup A WHERE A.Amount_in_USD IN (SELECT MAX(B.Amount_in_USD) FROM startup B WHERE B.Year = A.Year GROUP BY B.Year)"
    )

    // Execute each query and store the results in different folders
    queries.foreach { case (queryNumber, query) =>
      val resultDF = spark.sql(query)
      val outputFolder = s"/home/system/DZ/Scala/output/Q_$queryNumber"

      resultDF.write
        .option("header", true)
        .mode("overwrite")
        .csv(outputFolder)
    }
    
    modifiedDF7.show()
  }

  }
