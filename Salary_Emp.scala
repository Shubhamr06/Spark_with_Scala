import org.apache.spark.sql.{SparkSession, types}
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField


object Salary_Emp {

  def main (args:Array[String]):Unit={

    val spark=SparkSession.builder().appName("Max Salary").master("local[*]").getOrCreate()

    //val df = spark.read.textFile("Z:\\SparkProgrammingInScala-master\\01-HelloSpark\\src\\test\\scala\\salary_data")

    val emp = StructType(Array(
      StructField("ID",IntegerType,true),StructField("NAME",StringType,true),StructField("Salary",IntegerType,true)
    ))

    val df1=spark.read.format("CSV").schema(emp).load("Z:\\SparkProgrammingInScala-master\\01-HelloSpark\\src\\test\\scala\\salary_data")

    df1.show()
    df1.createOrReplaceTempView("employee")

    val maxsalDF= spark.sql("select * from employee")
    maxsalDF.show()





  }

}
