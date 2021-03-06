import java.sql.{DriverManager, ResultSet}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql._

object sample {
  def main(args: Array[String]): Unit = {

    // Required setup to create Spark RDDs

    val sparkConf = new SparkConf()
    val cnf = sparkConf.setMaster("local[*]")
    sparkConf.setAppName("Sample Tests")
    val sc = new SparkContext(cnf)
    // Textfile from Desktop is converted into an RDD

    val rdd = sc.textFile("D:\\Users\\sseth\\Desktop\\Book1.csv")
    // All the elements from the RDD are printed
    rdd.foreach(x => println(x))
    // A Dataframe is created from an XML file; a jar was added to support xml reading capabilities

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val df2 = sqlContext.read.format("com.databricks.spark.xml").option("rowTag", "CPRBody").load("D:\\Users\\sseth\\Desktop\\CPR905.xml")

    // The filtered RDD has selected 3 columns from the fd2 dataframe
    val filteredDF = df2.select("ISIN", "RptDt", "BusDt")
    filteredDF.show()

    df2.select("ISIN").map(x => x.toString().substring(0,3)).foreach(println)

    df2.select("ISIN").map(x => x.toString().length == 14).foreach(println)

    // An RDD is created and filter criteria are applied to the newly created RDD

    val lengthRec = filteredDF.map(x => records(x.getAs[String]("ISIN"), x.getAs[String]("RptDt"), x.getAs[String]("BusDt")))
    .filter(rec => rec.BusDt > "2014-05-06")
    .filter(rec => rec.ISIN.toString.length == 12)

    println("The RDD starts here")
    lengthRec.foreach(x => println(x))
    withCountry.foreach(x =>println(x))
    println("The RDD ends here")

    val finalDF = sqlContext.createDataFrame(lengthRec)

    // A new column is added to this dataframe
    val modDF = finalDF.withColumn("Country Code", finalDF("ISIN").substr(0,2))

    println("The final dataframe starts here")
    finalDF.show()
    println("The final dataframe ends here")


    println("The dataframe with an added column starts here")
    modDF.show()
    println("The dataframe with an added column ends here")


    // Table extracted from MySQL database is converted into a dataframe
     val jdbcDF = sqlContext.read.format("jdbc").options(
       Map("url" -> "jdbc:mysql://localhost/test?user=root&password=mysql",
       "dbtable" -> "test.cprValid4")).load()
       jdbcDF.show()

  }

   def createConnection() = {
   Class.forName("com.mysql.jdbc.Driver").newInstance();
   DriverManager.getConnection("jdbc:mysql://localhost/test?user=root&password=mysql");
   }

   def extractValues(r: ResultSet) = {
   (r.getInt(1), r.getString(2))
   }

   def filterByLength(l: String): Boolean = {
   if (l.length == 14) true else false
   }

}
// created a case class to store the elements of the dataframe
case class records(ISIN: String, RptDt: String, BusDt: String)

