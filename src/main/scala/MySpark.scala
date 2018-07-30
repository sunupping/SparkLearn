import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jie.sun on 2018/7/30.
  */
object MySpark {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("mySpark")
    //setMaster("local") 本机的spark就用local，远端的就写ip
    //如果是打成jar包运行则需要去掉 setMaster("local")因为在参数中会指定。
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    val data = Array(1,2,3,4,5,6)
    //使用累加器方法来避免
    val accum = sc.longAccumulator("Ede Accumulator")
    val distData = sc.parallelize(data) //sc.parallelize(data,10) 第二个参数代表分区数，每个分区会对应一个任务
//    var counter = 0
    distData.foreach(x => {
      accum.add(x)
    })
    println("Counter value: "+accum.value)

   // val rdd = sc.parallelize(List(1,2,3,4,5,6).map(_*3))
   // val mappedRDD = rdd.filter(_>10).collect()

    //对集合求和
    //println(distData.reduce(_+_)*3)
    //输出大于10的元素
//    for(arg <- mappedRDD){
//      print(arg+" ")
//    }
    println()
    println("math is work")


  }
}
