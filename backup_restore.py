# Importar las bibliotecas necesarias
from pyspark.sql import SparkSession
from datetime import datetime
import avro.schema, avro.io, avro.datafile
import parameters
import pandas as pd
import sqlite3
import json



def generate_backup(table_name):
    # SparkSession creation
    spark = SparkSession.builder.appName("backup_avro") \
        .config("spark.jars.packages", "org.xerial:sqlite-jdbc:3.34.0") \
        .config("spark.driver.extraClassPath", "dependencies/sqlite-jdbc-3.34.0.jar") \
        .getOrCreate()

    # SQLite table to DataFrame
    df = spark.read.format("jdbc") \
        .option("url", f"jdbc:sqlite:{parameters.DB_FILE}") \
        .option("dbtable", table_name) \
        .option("driver", "org.sqlite.JDBC") \
        .load()
    df_json = df.toJSON().collect()

    # create file name with date and time
    now = datetime.now()
    date_string = now.strftime("%Y%m%d_%H%M%S")
    filename = f"{parameters.BACKUP_FOLDER}/{table_name}_{date_string}.avro"
    fp = open(filename, 'w+b')

    # write on AVRO file
    avro_schema = parameters.get_avro_schema(df, "record", "com." + table_name, table_name)
    aschema = avro.schema.parse(avro_schema)

    # Create an DatumWriter object base on schema
    writer = avro.io.DatumWriter(aschema)

    # Create a DataFileWriter object
    df_writer = avro.datafile.DataFileWriter(fp, writer, aschema)

    # Write date on Avro file
    for r in df_json:
        df_writer.append(json.loads(r))

    # Close Avro file
    df_writer.close()

    # Close SparkSession
    spark.stop()


def restore_backup(full_file_name, table_name):
    # SparkSession creation
    spark = SparkSession.builder.appName("restore_avro") \
        .config("spark.jars.packages", "org.xerial:sqlite-jdbc:3.34.0") \
        .config("spark.driver.extraClassPath", "dependencies/sqlite-jdbc-3.34.0.jar") \
        .getOrCreate()

    # read AVRO file
    with open(full_file_name, 'rb') as f:
        reader = avro.datafile.DataFileReader(f, avro.io.DatumReader())
        df_reader = [rows for rows in reader]
        reader.close()

    df = pd.DataFrame(df_reader)
    #display(df)
    
    # restore table
    conn = sqlite3.connect(parameters.DB_FILE)
    df.to_sql(table_name,conn,if_exists='replace',index=False)
    conn.commit()
    conn.close()


def truncate_table(table_name):
    # Truncate table
    conn = sqlite3.connect(parameters.DB_FILE)
    cursor = conn.cursor()
    cursor.execute(f'DELETE FROM {table_name}')    
    conn.commit()
    conn.close()


if __name__ == '__main__':
    #pass
    generate_backup("departments")    
    #restore_backup("data/backups/AVRO/departments_20230221_194950.avro", "departments")
    #truncate_table("departments")