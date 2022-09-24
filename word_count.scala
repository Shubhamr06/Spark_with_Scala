import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
object word_count {

  def main(args: Array[String]): Unit = {

  val conf= new SparkConf()
    .setMaster("local[*]")
    .setAppName("wordcount")

    val sc = new SparkContext(conf)

    val rdd=sc.textFile("Z:\\SparkProgrammingInScala-master\\01-HelloSpark\\src\\test\\scala\\word_ct_Data")
    rdd.foreach(println)


    val rdd1=rdd.flatMap(x=>x.split(" "))
      .map(x=>(x,1))
      .reduceByKey((x,y)=>(x+y))

    rdd1.foreach(println)

    rdd1.saveAsTextFile("Z:\\SparkProgrammingInScala-master\\01-HelloSpark\\src\\test\\scala\\word_ct_Data2")

  }
}
