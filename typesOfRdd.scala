import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
object typesOfRdd {

  def main(args:Array[String]):Unit={

    val conf = new SparkConf().
      setMaster("local[*]").
      setAppName("typeofRDD")

    val sc = new SparkContext(conf)

    //parrlelize method
    val rdd=sc.parallelize(Seq(1,2,3,4))
    val rdd1=sc.makeRDD(Seq(1,2,3,4,4))
    rdd.foreach(println)
    rdd1.foreach(println)

    //map-partition rdd







  }
}
