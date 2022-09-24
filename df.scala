import org.apache.spark
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.SparkSession


object df {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[3]")
      .appName("HelloSparkTest")
      .getOrCreate()


    val employee_sc = StructType(Array
    (StructField("Id",IntegerType,true)
      ,StructField("Name",StringType,true)
      ,StructField("company",StringType,true)
      ,StructField("salary",IntegerType,true)))

    val df =spark.read.format("CSV")
      .schema(employee_sc)
      .load("Z:\\SparkProgrammingInScala-master\\01-HelloSpark\\src\\test\\scala\\employee_data")

    df.show()


  }
}