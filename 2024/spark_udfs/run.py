from pyspark.sql import SparkSession, types as t, functions as f
import pandas as pd

spark = (
    SparkSession
    .builder
    .config("spark.jars", "add-one.jar,add-one-scala.jar")
    .getOrCreate()
)

schema = t.StructType([t.StructField("integers", t.IntegerType(), True)])
df = spark.createDataFrame([
    (i,) for i in range(100)
], schema=schema)


# Native
df_native = df.withColumn("integersPlusOne", f.col("integers") + f.lit(1))
df_native.show()


# Python UDF
@f.udf(returnType=t.IntegerType())
def add_one_udf(num: int) -> int:
    return num + 1


df_python_udf = df.withColumn("integersPlusOne", add_one_udf(f.col("integers")))
df_python_udf.show()


# Pandas UDF
@f.pandas_udf(t.IntegerType())
def add_one_pandas_udf(nums: pd.Series) -> pd.Series:
    return nums + 1


df_pandas_udf = df.withColumn("integersPlusOne", add_one_pandas_udf(f.col("integers")))
df_pandas_udf.show()


# Java UDF
df.createOrReplaceTempView("df")
spark.udf.registerJavaFunction("addOne", "sparkudfs.AddOne", t.IntegerType())
df_java_udf = spark.sql("SELECT integers, addOne(integers) AS integersPlusOne FROM df")
df_java_udf.show()


# Scala UDF
df.createOrReplaceTempView("df")
spark.udf.registerJavaFunction("addOneScala", "scalaudfs.AddOne", t.IntegerType())
df_scala_udf = spark.sql("SELECT integers, addOneScala(integers) AS integersPlusOne FROM df")
df_scala_udf.show()
