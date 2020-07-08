




  import org.apache.spark.sql.{SQLContext, SparkSession}

  object SCALAHIVE {
    def main(args: Array[String]) = {
      val spark = SparkSession
        .builder()
        .appName("mapExample")
        .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse")
        .config("hive.metastore.uris", "thrift://localhost:9083")
        .master("local[*]")
        .enableHiveSupport()
        .getOrCreate()

      val sqlcontext: SQLContext = spark.sqlContext


      val b =spark.sql("select * from cust1").toDF()
      b.printSchema()
      b.show()
    }




}
