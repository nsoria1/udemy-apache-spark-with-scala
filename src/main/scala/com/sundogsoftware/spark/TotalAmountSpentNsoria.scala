package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.log4j._

object TotalAmountSpentNsoria {

  def parseLine(line: String): (Int, Float) = {
    val fields = line.split(",")
    val id = fields(0).toInt
    val amount = fields(2).toFloat
    (id, amount)
  }

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local", "TotalAmountSpentNsoria")

    val input = sc.textFile("data/customer-orders.csv")

    val data = input.map(parseLine)

    val totalAmount = data.reduceByKey( (x,y) => x + y )

    val sorted = totalAmount.sortBy(_._2, ascending=false)

    for (result <- sorted) {
      val id = result._1
      val amount = result._2
      println(s"$id: $amount")
    }
  }
}
