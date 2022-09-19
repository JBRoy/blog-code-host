package com.zeotap.merge.dp.poc

import scala.collection.mutable


object Deltalake {

  type BlobPath = String
  type Format = String

  val snapshot = "snapshot"
  val update = "updates"

  val snapshotRow = Row("id_mid_10", "133900b3-44f1-43fe-873f-f4bde3dfd6af", 25, 35, "Female")
  val allFilledUpdate = Row("id_mid_10", "133900b3-44f1-43fe-873f-f4bde3dfd6af", 45, 75, "Male")
  val allNullUpdate = Row("id_mid_10", "133900b3-44f1-43fe-873f-f4bde3dfd6af", null, null, null)
  val genderNullUpdate = Row("id_mid_10", "133900b3-44f1-43fe-873f-f4bde3dfd6af", 15, 55, null)
  val minAgeNullUpdate = Row("id_mid_10", "133900b3-44f1-43fe-873f-f4bde3dfd6af", null, 65, "Female")
  val maxAgeNullUpdate = Row("id_mid_10", "133900b3-44f1-43fe-873f-f4bde3dfd6af", 20, null, "Male")
  val ageNullUpdate = Row("id_mid_10", "133900b3-44f1-43fe-873f-f4bde3dfd6af", null, null, "Female")
  val MaxAgeAndGenderNullUpdate = Row("id_mid_10", "133900b3-44f1-43fe-873f-f4bde3dfd6af", 23, null, null)
  val columns = List(("Demographic_MinAge", IntegerType), ("Demographic_MaxAge", IntegerType),
    ("Demographic_Gender", StringType)
  )

  def createDeltaTableFromSingleRow(path: String, r: Row)(implicit spark: SparkSession) = {
    createDeltaTableFromDataframe(createDataframeFromSingleRow(r), path)
    path
  }


  def createDataframeFromSingleRow(r: Row)(implicit spark: SparkSession) = {
    val schema = StructType(Seq(
      StructField("id_type", StringType, true),
      StructField("id", StringType, true),
      StructField("Demographic_MinAge", IntegerType, true),
      StructField("Demographic_MaxAge", IntegerType, true),
      StructField("Demographic_Gender", StringType, true),
    ))
    spark.createDataFrame(spark.sparkContext.parallelize(Seq(r)), schema)
  }

  val id_type_condition = joinCondition(snapshot, update, "id_type")
  val id_condition = joinCondition(snapshot, update, "id")

  def generatedDataframeUseCase(path: String)(implicit spark: SparkSession) = {
    val deltaTable = DeltaTable.forPath(spark, createDeltaTableFromSingleRow(path, snapshotRow)).as(snapshot)
    println(">>>>>>>>>>STARTED AT>>>>>>>>>>>>>>>>")
    deltaTable.toDF.show(false)
    println(">>>>>>>>>>STARTED AT>>>>>>>>>>>>>>>>")
    val rows = List(
      allFilledUpdate,
      allNullUpdate,
      genderNullUpdate,
      minAgeNullUpdate,
      maxAgeNullUpdate,
      ageNullUpdate,
      MaxAgeAndGenderNullUpdate
    )
    val useCases: List[DataFrame] = rows.map(x => createDataframeFromSingleRow(x))

    useCases.foreach(cdc => {
      val mergeBuilder = deltaTable
        .as(snapshot)
        .merge(cdc.as(update), s"$id_condition and $id_type_condition")

      val updateSetWithConditions = getNVLMapUsingCoalesceAndWhenOnUpdateHasNullWithoutCast(update, columns)

      mergeBuilder.whenMatched()
        .update(updateSetWithConditions)
        .execute()

      println(">>>>>>>>>CDC DATA VIEW>>>>>>>>>>>>>")
      cdc.show(false)

      println(">>>>>>>>>>>>ACTUAL RESULT VIEW>>>>>>>>>>>>>")
      deltaTable.toDF.show(false)
      println(">>>>>>>>>>>>ACTUAL RESULT VIEW>>>>>>>>>>>>>")
    })


  }

  def createDeltaTableFromDataframe(df: DataFrame, path: String) = df.write.format("delta").save(path)

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    implicit val spark = SparkSession.builder()
      .master("local")
      .appName("DELTA LAKE NULL CHECK")
      .config(conf)
      .getOrCreate()
    spark.sparkContext.setLogLevel("OFF")

    generatedDataframeUseCase("/Users/joydeep/IdeaProjects/blog-code-host/src/main/resources/delta/testWhenExp/")

    println("debugger stops here!")

  }

  private def addCoalesceToMap(update: Format, d: mutable.Map[String, Column], name: (String, DataType)) = d +=
    (s"${name._1}" -> coalesce(col(s"${update}.${name._1}"), col(s"${snapshot}.${name._1}")))


  //3
  def partiallyCorrectImplUsingEqualToAndCoalesce(update: Format, columns: List[(String, DataType)]) = columns
    .foldLeft(mutable.Map[String, Column]())((d: mutable.Map[String, Column], name: (String, DataType)) => name._1 match {
      // This part is incorrect

      case "Demographic_MinAge" => d += (s"${name._1}" -> when(col(s"${update}.${name._1}").equalTo(lit(null).cast(name._2)), col(s"${snapshot}.${name._1}"))
        .otherwise(col(s"${update}.${name._1}")))

      // This part generates correct output but still not what we want

      case _ => addCoalesceToMap(update, d, name)
    })

  //4
  private def correctImplUsingWhenAndCoalesce(update: Format, columns: List[(String, DataType)]) = columns
    .foldLeft(scala.collection.mutable.Map[String, Column]())((d: mutable.Map[String, Column], name: (String, DataType)) => name._1 match {
      case "Demographic_MinAge" => d += (s"${name._1}" -> when(col(s"${update}.${name._1}").isNotNull, col(s"${update}.${name._1}"))
        .otherwise(col(s"${snapshot}.${name._1}")))
      case "Interest_IAB" => d
      case "Device_DeviceOS" => d
      case _ => addCoalesceToMap(update, d, name)
    })

  //2
  private def wrongImplUsingLiteralAndCast(update: Format, columns: List[(String, DataType)]) = columns
    .foldLeft(mutable.Map[String, Column]())((d: mutable.Map[String, Column], name: (String, DataType)) => {
      d += (s"${name._1}" ->
        when(col(s"${update}.${name._1}") === lit(null).cast(name._2),
          col(s"${snapshot}.${name._1}"))
          .otherwise(
            col(s"${update}.${name._1}")
          )
        )
    })

  //1
  private def wrongImplUsingEqualsOperator(update: String, columns: List[(String, DataType)]) = columns
    .foldLeft(mutable.Map[String, Column]())((d: mutable.Map[String, Column], name: (String, DataType)) => {
      d += (s"${name._1}" ->
        when(col(s"${update}.${name._1}") === null,
          col(s"${snapshot}.${name._1}"))
          .otherwise(
            col(s"${update}.${name._1}")
          )
        )
    })

  def joinCondition(snapshot: String, updates: String, column: String): String = s"$snapshot.$column = $updates.$column"

}
