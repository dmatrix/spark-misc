import sys
sys.path.append('.')
import warnings
warnings.filterwarnings("ignore")

from pyspark.sql import SparkSession
from pyspark.sql.functions import udtf
from pyspark.sql.connect.udtf import UserDefinedTableFunction, PythonEvalType, AnalyzeArgument,AnalyzeResult
from pyspark.sql.types import Row, StructType, StructField, IntegerType, StringType

# Define a simple polymorphic UDTF class

@udtf()
class ProcessPolymorphicSimpleData(UserDefinedTableFunction):
    evalType = PythonEvalType.SQL_TABLE_UDF

    @staticmethod
    def analyze(input_schema: AnalyzeArgument):
      if isinstance(input_schema, int):
        return AnalyzeResult(StructType([
            StructField("input_number", IntegerType(), True),
            StructField("squared", IntegerType(), True)
        ]))
      elif isinstance(input_schema, str):
        return AnalyzeResult(StructType([
            StructField("info", StringType(), True),
            StructField("length", IntegerType(), True)
        ]))
      else:
        return AnalyzeResult(StructType([
            StructField("words", StringType(), True),
            StructField("length", IntegerType(), True)
        ]))

    def eval(self, row: Row):
        if 'value' in row:
            yield (row['value'], row['value'] ** 2)
        else:
            info = row['info']
            yield (info, len(info))

    # def terminate(self) -> Iterator[Any]:
    #   yield "done", None


if __name__ == "__main__":
    spark = (SparkSession.builder 
            .remote("local[*]") 
            .appName("PySpark UDTF Polymorphic Example 1")
            .getOrCreate())

    # Ensure we are conneccted to the spark session
    assert("<class 'pyspark.sql.connect.session.SparkSession'>" == str(type((spark))))
    print(f"+++++Making sure it's using SparkConnect session:{spark}+++++")

    df_1 = spark.createDataFrame([(1,), (2,), (3,),(4,), (5,)], ["input_number"])
    df_2 = spark.createDataFrame([("Spark Connect"), ("with"), ("user defined table functions"), ("rocks!")], ["words"])
    
    print(df_1.show(truncate=False))
    print(df_2.show(truncate=False))

    df_1.createOrReplaceTempView("table_int_values")
    df_2.createOrReplaceTempView("table_string_values")

    spark.udtf.register("process_polymorphic_simple_data", ProcessPolymorphicSimpleData)
    print(spark.sql("SELECT * FROM process_polymorphic_simple_data(TABLE(table_int_values))").show())
    
