import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.ByteExactNumeric.mkOrderingOps

object rddExcersie {

  def main(args:Array[String]):Unit= {


    val conf = new SparkConf().setMaster("local[*]").setAppName("rddExcercise")
    val sc = new SparkContext(conf)

    val input_path = "Z:\\SparkProgrammingInScala-master\\01-HelloSpark\\src\\test\\scala\\word_ct_Data"
    val rdd = sc.textFile(input_path)
    rdd.foreach(println)

    val rdd1 = rdd.flatMap(_.split(" "))
    val rdd2 = rdd1.map(x => (x,x.length))
    rdd2.foreach(println)

    val rdd3 = rdd2.filter{case (x,y)=>y>=4}
    rdd3.foreach(println)


  }

}
