from pyspark.sql.types import StructField, StructType, IntegerType, StringType

def getschema(table_name):
    
   if table_name == "hired_employees":
      Structure = StructType(
          [
            StructField('id', IntegerType(), False),
            StructField('name', StringType(), False),
            StructField('datetime', StringType(), False),
            StructField('department_id', IntegerType(), False),
            StructField('job_id', IntegerType(), False)
          ]
      )

   elif table_name == "departments":
       Structure = StructType(
          [
            StructField('id', IntegerType(), False),
            StructField('department', StringType(), False)
         ]
      )

   elif table_name == "jobs":
      Structure = StructType(
         [
            StructField('id', IntegerType(), False),
            StructField('job', StringType(), False)
         ]
      )
   else:
      validation_msg = f'Error: table -> {table_name} <- not found in schemas.py'
      raise Exception(validation_msg)
      
   return Structure
