import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jie.sun on 2018/7/31.
  */
/**
  * Created by jie.sun on 2018/7/31.
  */
object RDD_api {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("spark-test").setMaster("local[2]")
    val sc = new SparkContext(conf)

    //word count
    val textFile = sc.textFile("hdfs://data02:8020/user/test1/input")
    val counts = textFile.flatMap(line => line.split(" ")).map(x => (x,1)).reduceByKey(_+_)
    counts.saveAsTextFile("hdfs://data02:8020/user/test1/output")

    //pi estimation
//    var NUM_SAMPLES = 1000000
//    val count = sc.parallelize(1 to NUM_SAMPLES).filter{_ =>
//      val x = math.random
//      val y = math.random
//      x*x + y*y < 1
//    }.count()
//    println(s"Pi is roughly ${4.0 * count / NUM_SAMPLES}")
  }


}
