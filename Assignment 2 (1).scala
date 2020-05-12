// Databricks notebook source
//////SECTION 2

// COMMAND ----------

//Question 1 - number 1
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType};
val df = spark.table("game_shanky_csv").select("game_id","season", "away_goals", "home_goals")
val df1 = df.withColumn("Total Goals", $"home_goals" + $"away_goals")
val df2 = df1.drop("home_goals").drop("away_goals")
df2.show(10)

// COMMAND ----------

//Question 1 - number 2
val df3 = df2.orderBy("season")
df3.show(10)

// COMMAND ----------

//Q1 - number 3
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
//define a window
val particionwindow= Window.partitionBy($"season")
//define average variable
val average = avg("Total Goals").over(particionwindow)
//define maximum variable
val maximum = max("Total Goals").over(particionwindow)
//define minimum variable
val minimum = min("Total Goals").over(particionwindow)
//define a transformation that needs to be applied within a window
val df4= df3.select($"*", average as "average goals", minimum as "minimum goals", maximum as "maximum goals")
df4.show(10)

// COMMAND ----------

//Q1 - number 4
//define a window
val particionwindow = Window.partitionBy($"season")
//define a transformation that needs to be applied within a window
val average =avg("Total goals").over(particionwindow)
val df5= df3.select($"*", average as "average goals")
val df6= df5.withColumn("Difference", $"Total Goals" - $"average goals")


// COMMAND ----------

//Q1 - number 5
df6.show(10)

// COMMAND ----------

// QUESTION 2
//PART 1
val df7 = spark.table("game_shanky_csv").select("season", "game_id", "venue", "away_team_id").filter($"venue"==="TD Garden" && ($"season"===20122013 || $"season"===20132014))
val df8 = spark.table("team_info_shanky_csv").select("teamName", "team_id")
val df9 = df7.join(df8, df7("away_team_id") === df8("team_id")).select("teamName")
df9.show()

// COMMAND ----------

//QUESTION 2 
//PART 2
df9.distinct().count()

// COMMAND ----------

// MAGIC 
// MAGIC %python
// MAGIC def prime(n):
// MAGIC   arr = []
// MAGIC   for num in range(1,n):
// MAGIC     if num > 1:
// MAGIC       for i in range(2,num):
// MAGIC         if num % i == 0:
// MAGIC           break 
// MAGIC       else:
// MAGIC         arr.append(num)
// MAGIC   return arr
// MAGIC prime(17)

// COMMAND ----------


