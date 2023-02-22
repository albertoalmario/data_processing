import os
from data_process import DataLoad
from parameters import ERRORS_FOLDER

SourceFile = 'data/source_files/jobs.csv'
#SourceFile = 'data/source_files/departments.csv'
#SourceFile = 'data/source_files/hired_employees.csv'

table_name = os.path.splitext(os.path.basename(SourceFile))[0]

dataload = DataLoad(SourceFile, table_name, 'csv')
print(f'Loading table {table_name}....')
dataload.csv_load()
dataload.apply_rules()
dataload.generate_erros_file(ERRORS_FOLDER)
dataload.save_on_db()
print(f'Source Count -> {dataload.data_frame.count()}')
print(f'Errors Count -> {dataload.errors_data_frame.count()}')
print(f'Target Count -> {dataload.final_data_frame.count()}')
dataload.spark.stop()

#dataload.data_frame.printSchema()
#dataload.errors_data_frame.show()
#dataload.final_data_frame.show()