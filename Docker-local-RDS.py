import csv
import pymysql

def main():
    # Initialize the database connection
    db_host = '**'
    db_user = '**'
    db_password = '**'
    db_name = '**'
    connection = pymysql.connect(host=db_host, user=db_user, password=db_password, db=db_name, connect_timeout=5)

    try:
        with connection.cursor() as cursor:
            # Drop the table if it exists
            cursor.execute("DROP TABLE IF EXISTS startup")
            
            # Create a table (if not exists) to store the CSV data
            create_table_query = '''
                CREATE TABLE startup(
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

            # Read the CSV file and insert the data into the RDS table
            with open('/app/startup.csv') as csv_file:
                csv_reader = csv.reader(csv_file)
                next(csv_reader)  # Skip the header row

                data_to_insert = []
                for row in csv_reader:
                    if len(row) >= 10:
                        Sr_No = row[0]
                        Date = row[1]
                        Startup_Name = row[2]
                        Industry_Vertical = row[3]
                        SubVertical = row[4]
                        City = row[5]
                        Investors_Name = row[6]
                        InvestmentnType = row[7]
                        Amount_in_USD = row[8]
                        Remarks = row[9]

                        data_to_insert.append((Sr_No, Date, Startup_Name, Industry_Vertical, SubVertical, City, Investors_Name, InvestmentnType, Amount_in_USD, Remarks))
                    else:
                        print("Skipped row due to insufficient columns:", row)

                # Bulk insert data into the table
                insert_query = "INSERT INTO startup (Sr_No, Date, Startup_Name, Industry_Vertical, SubVertical, City, Investors_Name, InvestmentnType, Amount_in_USD, Remarks) " \
                               "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
                cursor.executemany(insert_query, data_to_insert)

            # Commit the changes
            connection.commit()

    finally:
        # Close the database connection
        connection.close()

if __name__ == "__main__":
    main()
