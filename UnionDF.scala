import org.apache.spark.sql.SparkSession
object UnionDF {

  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder().master("local[*]").appName("UnionDF").getOrCreate()

    import spark.sqlContext.implicits._
    val df1 = Seq((101, "Shubham"), (102, "Rai")).toDF("ID", "Name")
    val df2 = Seq((101, "Shubham"), (103, "vishal")).toDF("ID", "Name")
    df1.show()
    df2.show()

    df1.union(df2).distinct().show()
    df1.unionAll(df2).show()
  }
}