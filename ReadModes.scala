import org.apache.spark.sql.SparkSession
object ReadModes {

  def main(args:Array[String]):Unit={

    val spark=SparkSession.builder().master("local[*]").appName("Read modes").getOrCreate()

    val path="Z:\\SparkProgrammingInScala-master\\01-HelloSpark\\src\\test\\scala\\curruptDATA.csv"
//    val data=spark.read.option("mode","FAILFAST").option("header",true).csv(path)    //Failfast mode
//    val data=spark.read.option("mode","DROPMALFORMED").option("header",true).csv(path) //Dropmalformed
    val data=spark.read.option("header",true).csv(path) //premissive mode
    data.show()
  }
}
