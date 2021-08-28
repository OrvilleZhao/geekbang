package com.orville

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import scala.collection.mutable.MutableList
import org.apache.hadoop.fs.FileUtil

object DistCp {
  def main(args: Array[String]): Unit = {
    val s_path = "/Users/jiying/myMobileDisk/source"
    val t_path = "/Users/jiying/myMobileDisk/target"
    val isignore = true
    val maxMap = 10
    val conf = new SparkConf().setAppName("distcp")
    //创建SparkContext,该对象是提交spark app的入口
    val sc = new SparkContext(conf);
    //遍历子目录，人如果不存在目录则创建
    FileSystem.get(sc.hadoopConfiguration).listStatus(new Path(s_path)).foreach(x=>
      if(x.isDirectory){
        val sub_path = new Path(t_path+x.getPath.toString.split(s_path)(1))
        if (!FileSystem.get(sc.hadoopConfiguration).exists(sub_path)){
           FileSystem.get(sc.hadoopConfiguration).mkdirs(sub_path)
        }
    })
    val sIterator = FileSystem.get(sc.hadoopConfiguration).listFiles(new Path(s_path),true)
    val fileList = MutableList[(String, String)]()
    while(sIterator.hasNext()){
      val s = sIterator.next()
      val targetPath = t_path+s.getPath.toString.split(s_path)(1)
      val tuple = (s.getPath.toString, targetPath)
      fileList += tuple
    }
    val rdd = sc.makeRDD(fileList, maxMap)
    rdd.mapPartitions(copyMapper).collect
  }
}

def copyMapper(iter: Iterator[(String, String)]) = 
{ 
    while (iter.hasNext)
    {
      val f: (String, String) = iter.next()
      val f1 = new Path(f._1)
      val f2 = new Path(f._2)
      FileUtil.copy(f1.getFileSystem(sc.hadoopConfiguration),f1,f2.getFileSystem(sc.hadoopConfiguration),f2,false,sc.hadoopConfiguration)
    }
    // return Iterator[U]
    Iterator.empty 
}