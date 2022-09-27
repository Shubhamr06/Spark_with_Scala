import org.apache.spark.sql.SparkSession
object doubleDelimater {
 def main(args:Array[String]):Unit={

   val spark=SparkSession.builder().master("local[*]").appName("Read modes").getOrCreate()

   val path="Z:\\SparkProgrammingInScala-master\\01-HelloSpark\\src\\test\\scala\\delimated_data.csv"
   val data=spark.read.option("delimiter",",!|").option("header",true).csv(path)
   data.show()
 }
}
