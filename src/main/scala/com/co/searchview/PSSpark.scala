package com.co.searchview

import org.apache.spark._
import scala.math.random

/*
 * First crack at a Spark version of Search-View matching
 * Make sure to fill in the domain for hdfs
 */
object PSSpark {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SearchViewSpark")
    val spark = new SparkContext(conf)

    // read in search results
    val file = spark.textFile("hdfs://<domain>:8020/user/hduser/data/search_results*");

    // split along ctrl-a, filter out unknown cookies
    val split1 = file.map(line => line.split("\u0001"))
    val f1 = split1.filter(r => r(3) != "UNKNOWN")

    // our search result class
    case class SR (macid : String, time: Long, resStr: String);

    // send the data into the class
    val split2 = f1.map(r => (r(3), SR(r(3), r(1)toLong, r(7))))

    // read in ad views
    val vip = spark.textFile("hdfs://<domain>:8020/user/hduser/data/viewed_ad.*");

    // split along ctrl-a, filter out unknown cookies
    val vip1 = vip.map(_.split("\u0001"))
    val vf1 = vip1.filter(v => v(4) != "UNKNOWN");

    // Declare and populate our View class
    case class VIP(macid: String, time: Long, adid: Long)
    val vip2 = vf1.map(v => (v(4), VIP(v(4), v(1).toLong, v(3).toLong)))

    //vip2.collect()

    // do the join
    val srvip = split2.join(vip2)

    // diagnostics
    //val srvip = split2.join(vip2).take(10)
    //srvip.take(2);

    // Get search-view records less than 30 minutes apart
    val ff1 = srvip.filter(v => (v._2._1.time - v._2._2.time) < (1800L * 1000L))
    val ff2 = ff1.take(10);

    //srvip.collect()
    //val srvip_final = srvip.map(r => (r(0), r(1).SR.time).take(5));

    ff1.saveAsTextFile("hdfs://<domain>:8020/user/hduser/jobs/srvip");

    println("Finished SearchViewSpark")
    spark.stop()
  }
}
