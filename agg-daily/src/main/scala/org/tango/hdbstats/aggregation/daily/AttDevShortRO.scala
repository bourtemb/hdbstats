package org.tango.hdbstats.aggregation.daily

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SaveMode
import scala.util.matching.Regex

object AttDevShortRO {

  val CASSANDRA_HOST: String = "160.103.208.75"
  val SPARK_MASTER: String = "spark://160.103.208.75:7077"
  val DAY_PATTERN: Regex = """([0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9])""".r

  def main(args: Array[String]): Unit = {
    if(args.size != 1) {
      println(
        """ *********************************************************************
          |
          | Usage : org.tango.hdbstats.aggregation.daily.AttDevShortRO yyyy-mm-dd
          |
          | *********************************************************************
        """.stripMargin)
    } else {
      val input = args(0)
      input match {
        case DAY_PATTERN(day) => compute(day)
        case _ => throw new IllegalArgumentException(s" The input date $input " +
          s"should comply to the date pattern ${DAY_PATTERN.toString}")
      }
    }
  }

  def compute(day: String): Unit = {

    val conf = new SparkConf(true)
      .set("spark.app.name","AttDevShortRO")
      .set("spark.cassandra.connection.host", CASSANDRA_HOST)

    val sc = new SparkContext(SPARK_MASTER, "AttDevShortRO", conf)
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

    val devShortRoTable = sqlContext
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "att_scalar_devshort_ro", "keyspace" -> "hdbtest"))
      .load()

    devShortRoTable.registerTempTable("att_scalar_devshort_ro")

    val devShortRo = sqlContext
      .sql(s"""
        SELECT "DAY" AS type_period, att_conf_id, period, count(att_conf_id) AS count_point, count(error_desc) AS count_error,
        count(DISTINCT error_desc) AS count_distinct_error,
        min(value_r) AS value_r_min, max(value_r) AS value_r_max, avg(value_r) AS value_r_mean, stddev(value_r) AS value_r_sd
        FROM att_scalar_devshort_ro
        WHERE period="${day}"
        GROUP BY att_conf_id, period""")

    devShortRo.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> "stat_scalar_devshort_ro", "keyspace" -> "hdbtest"))
      .mode(SaveMode.Append)
      .save()

    sc.stop()
  }


}
