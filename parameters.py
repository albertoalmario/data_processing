"""
@author: Alberto Almario
@email: albertoalmario@gmail.com
"""
from pyspark.sql.types import StructField, StructType, IntegerType, StringType
import json

SOURCE_FOLDER = 'data/source_files'
ERRORS_FOLDER = 'data/processed_files/errors'
BACKUP_FOLDER = 'data/backups/AVRO'
DB_FILE = 'data/database/globant.sqlite'

def getschema(table_name):
   """
   Returns the corresponding StrucType of a table
   """
    
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


def get_avro_schema(spark_df, schema_type: str, name: str, namespace: str):
   """
   Returns the corresponding avro schema of spark dataframe.
   """

   schema_base = {
      "type": schema_type,
      "namespace": name,
      "name": namespace
   }

   # Dictionary to map Spark types to Avro types
   avro_mapping = {
      'StringType()': ["string", "null"],
      'LongType()': ["long", "null"],
      'IntegerType()': ["int", "null"],
      'BooleanType()': ["boolean", "null"],
      'FloatType()': ["float", "null"],
      'DoubleType()': ["double", "null"],
      'TimestampType()': ["long", "null"],
      'ArrayType(StringType(),true)': [{"type": "array", "items": ["string", "null"]}, "null"],
      'ArrayType(IntegerType(),true)': [{"type": "array", "items": ["int", "null"]}, "null"]
   }

   fields = []

   for field in spark_df.schema.fields:
      if (str(field.dataType) in avro_mapping):
         fields.append({"name": field.name, "type": avro_mapping[str(field.dataType)]})
      else:
         fields.append({"name": field.name, "type": str(field.dataType)})

   schema_base["fields"] = fields

   return json.dumps(schema_base)