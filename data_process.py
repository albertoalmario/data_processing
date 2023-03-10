"""
@author: Alberto Almario
@email: albertoalmario@gmail.com
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lit
from datetime import datetime
import functools
import parameters
import sqlite3


class DataLoad():
    """
    This class load files to a database using pyspark and pandas.
    """
    def __init__(self, p_source_file, p_table_destination,p_source_type='csv'):
        self.source_file=p_source_file
        self.table_destination=p_table_destination
        self.source_type=p_source_type
        self.table_schema=parameters.getschema(self.table_destination)
        self.data_frame=""
        self.final_data_frame=""
        self.errors_data_frame=""
        
    def csv_load(self):
        # SparkSession creation
        self.spark = SparkSession.builder.appName("data_processing")\
            .config('spark.master','local[4]')\
            .getOrCreate()
        # load file in format csv
        self.data_frame = self.spark.read.csv(self.source_file, self.table_schema, header=False)
    
    def apply_rules(self):
        # get dataframe columns
        df_columns = [col(c) for c in self.data_frame.columns]
        # using reduce and lambda methods to create null filter for all columns
        null_filter = functools.reduce(lambda a, b: a | b.isNull(), df_columns, lit(False))
        # apply null filter to get rows with null data
        self.errors_data_frame = self.data_frame.where(null_filter)
        # get rows without null data
        self.final_data_frame = self.data_frame.na.drop()

    def generate_erros_file(self, output_folder):
        if self.errors_data_frame.count() > 0:
            # set errors file name
            now = datetime.now()
            date_string = now.strftime("%Y%m%d_%H%M%S")
            errors_filename = f"{output_folder}/errors_table_{self.table_destination}_{date_string}"
            # generate csv file with errors
            # self.errors_data_frame.write.options(header=True, delimiter='|', mode='overwrite').csv(errors_filename) # Spark Version
            self.errors_data_frame.toPandas().to_csv(f'{errors_filename}.csv', sep='|', header=True, index=False)
            print(f'!!..errors file generate: --> {errors_filename}')
        else:
            print('no errors found')

    def save_on_db(self):
        conn = sqlite3.connect(parameters.DB_FILE)
        self.final_data_frame.toPandas().to_sql(self.table_destination,conn,if_exists='replace',index=False)
        conn.commit()
        conn.close()

if __name__ == '__main__':
    pass