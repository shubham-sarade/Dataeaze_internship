import argparse
import boto3
from pyspark.sql import SparkSession

def word_count(input_file, output_file):
    # Your code to read the text content from S3
    s3 = boto3.client('s3')
    bucket_name = '***'
    obj = s3.get_object(Bucket=bucket_name, Key=input_file)
    text_content = obj['Body'].read().decode('utf-8')

    # Create a SparkSession
    spark = SparkSession.builder \
        .appName("WordCount") \
        .getOrCreate()

    # Suppress unnecessary logs
    spark.sparkContext.setLogLevel("ERROR")

    # Create RDD from text content
    lines_rdd = spark.sparkContext.parallelize([text_content])

    # Perform word count
    word_counts_rdd = lines_rdd.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

    # Save word counts to CSV file directly without using DataFrame
    word_counts_rdd.saveAsTextFile(output_file)

    # Stop the SparkSession
    spark.stop()

    # Initialize S3 client
    s3 = boto3.client('s3')

    # Upload the CSV file to S3
    s3.upload_file(output_file + "/part-00000", bucket_name, output_file)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PySpark Word Count")
    parser.add_argument("--input", required=True, help="Input file path in S3")
    parser.add_argument("--output", required=True, help="Output file path in S3")
    args = parser.parse_args()

    word_count(args.input, args.output)



