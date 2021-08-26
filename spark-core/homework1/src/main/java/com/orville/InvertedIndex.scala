package com.orville

import org.apache.spark.{SparkConf, SparkContext}

object InvertedIndex {

  def main(args: Array[String]): Unit = {
    //创建SparkConf()并且设置App的名称
    val conf = new SparkConf().setAppName("invertedIndex");
    //创建SparkContext,该对象是提交spark app的入口
    val sc = new SparkContext(conf);
    var rdd1 = sc.textFile("/Users/jiying/myMobileDisk/tmp/0").flatMap(line=>line.split(" ")).map(word=>(word,0))
    var rdd2 = sc.textFile("/Users/jiying/myMobileDisk/tmp/1").flatMap(line=>line.split(" ")).map(word=>(word,1))
    var rdd3 = sc.textFile("/Users/jiying/myMobileDisk/tmp/2").flatMap(line=>line.split(" ")).map(word=>(word,2))
    rdd1.union(rdd2).union(rdd3).map((_, 1)).reduceByKey((_ + _)).map(x => (x._1._1, (x._1._2, x._2))).sortBy(_._2).groupByKey().sortByKey().collect().foreach(x=>println(s"'${x._1}':{${x._2.mkString(",")}}"))
    //停止sc，结束该任务
    sc.stop();
  }
}
