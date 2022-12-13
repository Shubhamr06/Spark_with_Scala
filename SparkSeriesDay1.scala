import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object SparkSeriesDay1 {
//  1. Read the input testfile (Pipe delimited) provided as a "Spark RDD"
//  2. Remove the Header Record from the RDD
//    3. Calculate Final_Price:
//    Final_Price = (Size * Price_SQ_FT)
//  4. Save the final rdd as Textfile with three fields
//    Property_ID|Location|Final_Price

  def main(args:Array[String]):Unit={

    val conf=new SparkConf().setMaster("local[*]").setAppName("SparkSeriesDay1")
    val sc=new SparkContext(conf)


    val rdd=sc.textFile("Z:\\SparkProgrammingInScala-master\\01-HelloSpark\\src\\test\\scala\\input.txt")
    //removing header record from file
    val header=rdd.first()
    val data=rdd.filter(x=>x!=header)
    val finalPrice=data.map(x=>(x.replace("|",",")))
      .map(x=>(x.split(",")(0),x.split(",")(1),(
      x.split(",")(5).toFloat *
      x.split(",")(6).toFloat)).toString)
      .map(_.replaceAll("[()]","")).map(x=>(x.replace(",","|")))

    val newHeader=sc.parallelize(Array("Property_ID|Location|Final_Price"))
    val mergedRDD=newHeader.union(finalPrice)

    finalPrice.foreach(println)
    mergedRDD.foreach(println)





  }

}
