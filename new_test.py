from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import IntegerType, StringType
slen = pandas_udf(lambda s: s.str.len(), IntegerType())  
def to_upper(s):
    return s.str.upper()
def add_one(x):
    return x + 1

df = spark.createDataFrame([(1, "John Doe", 21)],("id", "name", "age"))
df.select(slen("name").alias("slen(name)"), to_upper("name"), add_one("age")).show() 