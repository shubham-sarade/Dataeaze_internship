import csv
import boto3
import pymysql

# AWS credentials and region configuration
aws_access_key_id = '**'
aws_secret_access_key = '**'
region_name = '**'

# S3 bucket and CSV file information
bucket_name = '**'
csv_file_key = 'shubham_sarade_EDP/startup.csv'

# RDS (MySQL) connection details
db_host = '**'
db_user = '**'
db_password = '**'
db_name = '**'

# Create an S3 client
s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=region_name)

# Initialize the database connection
connection = pymysql.connect(host=db_host, user=db_user, password=db_password, db=db_name, connect_timeout=5)

def create_table(connection):
    # Create the table (if not exists) to store the CSV data
    with connection.cursor() as cursor:
        create_table_query = '''
            CREATE TABLE IF NOT EXISTS startup(
                Sr_No VARCHAR(255),
                Date VARCHAR(255),
                Startup_Name VARCHAR(255),
                Industry_Vertical VARCHAR(255),
                SubVertical VARCHAR(255),
                City VARCHAR(255),
                Investors_Name VARCHAR(255),
                InvestmentnType VARCHAR(255),
                Amount_in_USD VARCHAR(255),
                Remarks VARCHAR(255)
            );
        '''
        cursor.execute(create_table_query)
        connection.commit()

def insert_data_to_rds(connection, csv_reader):
    with connection.cursor() as cursor:
        insert_query = "INSERT INTO startup (Sr_No, Date, Startup_Name, Industry_Vertical, SubVertical, City, Investors_Name, InvestmentnType, Amount_in_USD, Remarks) " \
                       "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
        for row in csv_reader:
            if len(row) >= 10:
                cursor.execute(insert_query, row[:10])
        connection.commit()

def main():
    create_table(connection)
    
    # Fetch the CSV file from S3
    response = s3_client.get_object(Bucket=bucket_name, Key=csv_file_key)
    csv_content = response['Body'].read().decode('utf-8')
    csv_reader = csv.reader(csv_content.splitlines())
    
    # Insert data into the RDS table
    insert_data_to_rds(connection, csv_reader)
    
    # Close the database connection
    connection.close()

if __name__ == "__main__":
    main()
