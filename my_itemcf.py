from pyspark.sql import SparkSession
from pyspark.sql import functions as F 
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .getOrCreate()

path = 'ratings.csv'
df = spark.read.load(path,
                     format='csv', sep=',', header='true')

df1 = df.groupBy('userId').agg(F.collect_set('movieId')) \
        .withColumnRenamed('collect_set(movieId)', 'itemid_set')

def co_split(itemid_set):
    result = []
    for i in range(0, len(itemid_set) - 2):
        for j in range(i + 1, len(itemid_set) - 1):
            result.append((i, j))
    return result

new_split = udf(co_split, ArrayType(ArrayType(IntegerType())))


#slen = udf(lambda s: len(s), IntegerType())

df2 = df1.select(new_split('itemid_set').name('itemid')) 
df3 = df2.select(explode(df2.itemid).alias('new')).groupBy('new').count()
#df2 = df1.collect()
#print(df2[:10])
df3.show()
#df.printSchema()