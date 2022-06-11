import org.apache.spark.{SparkConf, SparkContext}
object Main  {
  def main(args: Array[String]) = {
    val sparkConf = new SparkConf().setAppName("rddApi").setMaster("local[1]")
    val sc = new SparkContext(sparkConf)
	//调用Spark读取文件API,读取文件内容
    val wordRDD= sc.textFile("/Users/yangmao/wwwroot/java/test.text")
    //使用flatMap进行分词后展开
      .flatMap {
        line =>
        //以.做分词需要加转义符
          val array = line.split("\\.", 2)
          val bookName = array(0)
          array(1).split("\"")(1).split(" ").map(word => (bookName, word))
      }

    val kvRDD= wordRDD.map(kv => (kv._2, kv._1)).map((_, 1))
      .reduceByKey((x,y) => x + y)
      .map{case ((k,v),cnt) => (k,(v,cnt))}
      .groupByKey() //只分组不聚合
      .collect()
      .foreach(println)
  }
}
