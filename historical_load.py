from data_process import DataLoad

SourceFile = 'data/source_files/jobs.csv'
dataload = DataLoad(SourceFile, 'jobs', 'csv')
dataload.csv_load()
#print(dataload.data_frame.count())
#dataload.data_frame.printSchema()
dataload.apply_rules()
#dataload.errors_data_frame.show()
#dataload.final_data_frame.show()
#print(dataload.final_data_frame.count())
dataload.generate_erros_file('data/processed_files/errors/')