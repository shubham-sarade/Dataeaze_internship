import csv
import boto3
import pymysql

def lambda_handler(event, context):
    # AWS credentials and region configuration
    aws_access_key_id = '***'
    aws_secret_access_key = '***'
    region_name = '**'

    # S3 bucket and CSV file information
    bucket_name = '**'
    csv_file_key = '**'  # Update with your CSV file key

    # RDS (MySQL) connection details
    db_host = '**'
    db_user = '**'
    db_password = '**'
    db_name = '**'

    # Create an S3 client
    s3_client = boto3.client('s3',
                             aws_access_key_id=aws_access_key_id,
                             aws_secret_access_key=aws_secret_access_key,
                             region_name=region_name)

    # Download the CSV file from S3
    s3_client.download_file(bucket_name, csv_file_key,'/**')

    # Connect to the RDS (MySQL) instance
    connection = pymysql.connect(host=db_host,
                                 user=db_user,
                                 password=db_password,
                                 db=db_name,
                                 connect_timeout=5)

    try:
        with connection.cursor() as cursor:
            # Drop the table if it exists
            cursor.execute("DROP TABLE IF EXISTS startup")
            # Create a table (if not exists) to store the CSV data
            create_table_query = '''
           CREATE TABLE startup(
  		Startup_Name VARCHAR(255),
  		Industry_Vertical VARCHAR(255),
  		SubVertical VARCHAR(255),
  		City VARCHAR(255),
  		Investors_Name VARCHAR(255),
  		InvestmentnType VARCHAR(255),
  		Remarks VARCHAR(255)
						);
            '''
            cursor.execute(create_table_query)
            
            
            # Read the CSV file and insert the data into the RDS table
            with open('/tmp/small_data.csv') as csv_file:
                csv_reader = csv.reader(csv_file)
                next(csv_reader)  # Skip the header row
                
                for row in csv_reader:
                    print("CSV Row:", row) 
                    if len(row) >= 7:
                        Startup_Name = row[0]
                        Industry_Vertical = row[1]
                        SubVertical = row[2]
                        City = row[3]
                        Investors_Name = row[4]
                        InvestmentnType = row[5]
                        Remarks = row[6]
                        
                        insert_query = "INSERT INTO startup (Startup_Name, Industry_Vertical, SubVertical, City, Investors_Name, InvestmentnType, Remarks) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s);"
                        cursor.execute(insert_query, (Startup_Name, Industry_Vertical, SubVertical, City, Investors_Name, InvestmentnType, Remarks))
                    else:
                        print("Skipped row due to insufficient columns:", row)

            # Commit the changes
            connection.commit()

    finally:
        # Close the database connection
        connection.close()

