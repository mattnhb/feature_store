def get_nested_fields(schema, prefix="", delimiter="#"):
    nested_fields = []

    for field in schema.fields:
        full_name = f"{prefix}{field.name}"
        if isinstance(field.dataType, StructType):
            nested_fields.extend(
                get_nested_fields(field.dataType, full_name + delimiter, delimiter)
            )
        else:
            nested_fields.append(full_name)

    return nested_fields


from pyspark.sql import functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Python Spark SQL basic example").getOrCreate()

# Example Column object
column_obj = F.col("my_column_name")

print(f"{column_obj._jc.toString()=}")
# 'my_column_name'
