import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import scala.language.implicitConversions

object toDFMethod_Dataframe {

  def main(args: Array[String]): Unit = {

    val spark= SparkSession.builder().master("local[*]").appName("To DF Method").getOrCreate()

    //import spark.implicits._
    /// this import will used for conversions
    import spark.sqlContext.implicits._
    val data=Seq((10,"Shubham"),(20,"Vishal"),(30,"Ram")).toDF("ID","NAME")
    //data.printSchema()
    //data.show()
    val df2=data.withColumn("new",col("ID")*2)
    data.printSchema()
    data.show(false)
    df2.show()
    df2.printSchema()



  }

}
