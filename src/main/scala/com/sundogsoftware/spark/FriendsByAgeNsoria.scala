package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{round, avg}

object FriendsByAgeNsoria {

    case class Person(id: Int, name:String, age:Int, friends: Int)

    def main(args: Array[String]) {
        Logger.getLogger("org").setLevel(Level.ERROR)

        val spark = SparkSession
            .builder
            .appName("SparkNsoria")
            .master("local[*]")
            .getOrCreate()

        import spark.implicits._
        val data = spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv("data/fakefriends.csv")
            .as[Person]

        val friendsByAge = data.select("age", "friends")
        
        val avgByAge = friendsByAge
            .groupBy("age")
            .agg(round(avg("friends"), 2))
            .withColumnRenamed("round(avg(friends), 2)", "friends_avg")
            .sort("age")
            .show()

        spark.stop()
    }
}