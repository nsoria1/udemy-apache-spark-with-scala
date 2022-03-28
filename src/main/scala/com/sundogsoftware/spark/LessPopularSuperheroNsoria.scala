package com.sundogsoftware.spark

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, IntegerType, StringType}

object LessPopularSuperheroNsoria {
    case class SuperHeroNames(id: Int, name: String)
    case class SuperHero(value: String)

    def main(args: Array[String]) {
        Logger.getLogger("org").setLevel(Level.ERROR)

        val spark = SparkSession
            .builder
            .appName("LessPopularSuperhero")
            .master("local[*]")
            .getOrCreate()

        val superHeroNamesSchema = new StructType()
            .add("id", IntegerType, nullable = true)
            .add("name", StringType, nullable = true)

        import spark.implicits._
        val names = spark.read
            .schema(superHeroNamesSchema)
            .option("sep", " ")
            .csv("data/Marvel-names.txt")
            .as[SuperHeroNames]

        val lines = spark.read
            .text("data/Marvel-graph.txt")
            .as[SuperHero]

        val connections = lines
            .withColumn("id", split(col("value"), " ")(0))
            .withColumn("connections", size(split(col("value"), " ")) - 1)
            .groupBy("id").agg(sum("connections").alias("connections"))
        
        val minConnections = connections
            .agg(min("connections").alias("min_connections"))
            .select("min_connections")
            .first()
            .getLong(0)

        val filConnections = connections
            .filter($"connections" === minConnections)
            .join(names, connections("id") === names("id"), "inner")

        println("Printing a list of the less popular superhero:")
        
        filConnections
            .select("name", "connections")
            .show(filConnections.count().toInt)

        spark.close()
    }
}
