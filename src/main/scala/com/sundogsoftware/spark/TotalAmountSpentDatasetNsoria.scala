package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.functions.{round, sum, col}

object TotalAmountSpentDatasetNsoria {
    case class CustomerData(id: Int, item: Int, amount: Float)

    def main(args: Array[String]) {
      Logger.getLogger("org").setLevel(Level.ERROR)

      val spark = SparkSession
        .builder
        .appName("SparkNsoria")
        .master("local[*]")
        .getOrCreate()

      val customerSchema = new StructType()
        .add("id", IntegerType, nullable = true)
        .add("item", IntegerType, nullable = true)
        .add("amount", FloatType, nullable = true)

      import spark.implicits._
      val ds = spark.read
        .schema(customerSchema)
        .csv("data/customer-orders.csv")
        .as[CustomerData]

      ds
        .groupBy("id")
        .agg(round(sum("amount"), 2).alias("sum_amount"))
        .sort(col("sum_amount").desc)
        .show()

      spark.stop()
  }
}
