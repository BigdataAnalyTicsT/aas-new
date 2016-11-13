package com.cloudera.datascience.intro

import java.io.Serializable

import org.apache.spark.rdd.RDD
import org.apache.spark.util.StatCounter

/**
  * Created by tonye0115 on 2016/11/13.
  */

class NAStatCounter extends Serializable{
  val stats: StatCounter = new StatCounter
  var missing:Long = 0

  def add(x:Double):NAStatCounter={
    if(x.isNaN){
      missing += 1
    }else{
      stats.merge(x)
    }
    this
  }

  def merge(other:NAStatCounter):NAStatCounter={
    stats.merge(other.stats)
    missing += other.missing
    this
  }

  override def toString: String = {
    "stats: " + stats.toString + " NaN: " + missing
  }

}

object NAStatCounter extends  Serializable{
  def apply(x:Double) = new NAStatCounter().add(x)

  def statsWithMissing(rdd: RDD[Array[Double]]): Array[NAStatCounter] = {
    val nastats = rdd.mapPartitions((iter: Iterator[Array[Double]]) => {
      val nas: Array[NAStatCounter] = iter.next().map(d => NAStatCounter(d))
      iter.foreach(arr => {
        nas.zip(arr).foreach { case (n, d) => n.add(d) }
      })
      Iterator(nas)
    })
    nastats.reduce((n1, n2) => {
      n1.zip(n2).map { case (a, b) => a.merge(b) }
    })
  }

  def main(args: Array[String]) {
    val nas1: NAStatCounter = NAStatCounter(10.0)
    println(nas1)
    nas1.add(2.1)
    println(nas1)
    val nas2: NAStatCounter = NAStatCounter(Double.NaN)
    nas1.merge(nas2)
    println(nas1)

    println("用Map函数创建一组NAStatCounter")
    val arr: Array[Double] = Array(1.0,Double.NaN,17.29)
    arr.map(d => NAStatCounter(d)).foreach(println)

    println("zip把两个相同长度的数组组合在一起生成一个新的array,纵向数据stats")
    val nas11: Array[NAStatCounter] = Array(1.0,Double.NaN,17.29).map(d => NAStatCounter(d))
    val nas12: Array[NAStatCounter] = Array(2,Double.NaN,0.6).map(d => NAStatCounter(d))

   //nas11.zip(nas12).map( p => p._1.merge(p._2)).foreach(println)
    nas11.zip(nas12).map{case(a,b) => a.merge(b)}.foreach(println)

  }
}


