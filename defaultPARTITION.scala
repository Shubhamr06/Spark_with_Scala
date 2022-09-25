import org.apache.spark.sql.SparkSession
object defaultPARTITION {

  def main(args:Array[String]):Unit={

    val spark=SparkSession.builder().appName("default partition").master("local[*]").getOrCreate()

    val df = spark.read.option("header",true).csv("Z:\\SparkProgrammingInScala-master\\01-HelloSpark\\src\\test\\scala\\appdata.csv")
      .repartition(3) // hard coding no of partition for the df

    df.show(5)

    val part =df.rdd.getNumPartitions // checking number of partition
    println(part)

    spark.conf.set("spark.sql.shuffle.partitions",50) //changing default number of partition 

    val df1=df.groupBy("Category").count()
    df1.show()
    val part1=df1.rdd.getNumPartitions
    println(part1) // now checking number of partition after doing transformations.

    //spark shuffle properties in use

    val output=spark.conf.get("spark.sql.shuffle.partitions")
    println(output)

  }
}
