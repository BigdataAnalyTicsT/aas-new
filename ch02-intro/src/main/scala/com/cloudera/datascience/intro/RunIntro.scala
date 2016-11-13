package com.cloudera.datascience.intro

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by tonye0115 on 2016/11/13.
  */
object RunIntro {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("Intro").setMaster("local[*]"))
    val rawblocks: RDD[String] = sc.textFile(this.getClass.getResource("/").getPath+"linkage")
    rawblocks.foreach(println)

  }

}
