package com.cloudera.datascience.intro

import org.apache.spark.rdd.RDD
import org.apache.spark.util.StatCounter
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.Map
import scala.collection.immutable.IndexedSeq
import NAStatCounter._


/**
  * Created by tonye0115 on 2016/11/13.
  */

case class MatchData(id1:Int,id2:Int,scores:Array[Double],matched:Boolean)

case class Scored(md: MatchData, score: Double)

object RunIntro {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("Intro").setMaster("local[1]"))
    val rawblocks: RDD[String] = sc.textFile(this.getClass.getResource("/").getPath + "linkage")
    rawblocks.foreach(println)

    def isHeader(line: String) = line.contains("id_1")

    val noHeaderRDD: RDD[String] = rawblocks.filter(!isHeader(_))

    noHeaderRDD.foreach(println)

    def toDouble(s: String) = {
      if ("?".equals(s)) Double.NaN else s.toDouble
    }

    def parse(line: String) = {
      val pieces: Array[String] = line.split(",")
      val id1: Int = pieces(0).toInt
      val id2: Int = pieces(1).toInt
      val scores: Array[Double] = pieces.slice(2, 11).map(toDouble)
      val matched: Boolean = pieces(11).toBoolean
      MatchData(id1, id2, scores, matched)
    }

    val parsed: RDD[MatchData] = noHeaderRDD.map(line => parse(line))

   /* parsed.foreach(println)

    parsed.cache()
    println(parsed.count())
    println( parsed.take(5))*/

    //聚合
    println("##### 聚合")
    val grouped: RDD[(Boolean, Iterable[MatchData])] = parsed.groupBy(md => md.matched)
    grouped.mapValues(x => x.size).foreach(println)

    //创建直方图
    println("##### 创建直方图")
    val matchCounts: Map[Boolean, Long] = parsed.map(md => md.matched).countByValue()
    matchCounts.foreach(println)

    //排序
    println("##### 排序")
    val matchCountsSeq: Seq[(Boolean, Long)] = matchCounts.toSeq
    matchCountsSeq.sortBy(_._1).foreach(println)
    matchCountsSeq.sortBy(_._2).foreach(println)
    matchCountsSeq.sortBy(_._2).reverse.foreach(println)


    //连续变量的概要统计 stats 统计 mean均值，stdev标准差，极值（max,min)
    //标准差是一组数据平均值分散程度的一种度量。一个较大的标准差，
    // 代表大部分数值和其平均值之间差异较大；一个较小的标准差，代表这些数值较接近平均值。
    println("##### 连续变量的概要统计")
    val statCounter: StatCounter = parsed.map(md => md.scores(0)).stats()
    println(statCounter)

    val stats: IndexedSeq[StatCounter] = (0 until 9).map(i => {
      parsed.map(md => md.scores(i)).filter(!_.isNaN).stats()
    })

    stats.foreach(println)


    println("##### 要在Scala集合的纵向所有记录上执行Merge操作，可以使用reduce函数")
    val nasRDD: RDD[Array[NAStatCounter]] = parsed.map(md => md.scores.map(d => NAStatCounter(d)))
 /*   nasRDD.foreach(x=>{
      println("length："+x.length)
      x.foreach(println)
    })*/
    //逻辑具体按步骤分析见StatsWithMissing
    val reduced = nasRDD.reduce((n1, n2) => {
      n1.zip(n2).map { case (a, b) => a.merge(b) }
    })
    reduced.foreach(println)

    println("##### 把缺失值分析代码打包为一个函数，调用函数")
    val nasArray: Array[NAStatCounter] = statsWithMissing(parsed.map(_.scores))
    nasArray.foreach(println)

    println("##### 变量的选择和评分")
    val statsTrue = statsWithMissing(parsed.filter(_.matched).map(_.scores))
    println("statsTrue")
    statsTrue.foreach(println)

    val statsFalse = statsWithMissing(parsed.filter(!_.matched).map(_.scores))
    println("statsFalse")
    statsFalse.foreach(println)
    statsTrue.zip(statsFalse).map { case(m, n) =>
      (m.missing + n.missing, m.stats.mean - n.stats.mean)
    }.foreach(println)

    def naz(d: Double) = if (Double.NaN.equals(d)) 0.0 else d
    val ct = parsed.map(md => {
      val score = Array(2, 5, 6, 7, 8).map(i => naz(md.scores(i))).sum
      Scored(md, score)
    })

    ct.filter(s => s.score >= 4.0).
      map(s => s.md.matched).countByValue().foreach(println)
    ct.filter(s => s.score >= 2.0).
      map(s => s.md.matched).countByValue().foreach(println)

  }
}
