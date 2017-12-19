package com.hortonworks.spark.atlas.ml

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.scalatest.Matchers

class MLAgentSuite extends SparkFunSuite with Matchers {

  private var sparkSession: SparkSession = _

  //AgentML.loadAgent()

  override def beforeAll(): Unit = {
    super.beforeAll()
    sparkSession = SparkSession.builder()
      .master("local")
      .config("spark.sql.catalogImplementation", "in-memory")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    sparkSession.stop()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    super.afterAll()
  }

  test("hooker to spark ml pipeline fit") {

    val uri = "/"
    val pipelineDir = "tmp/pipeline"
    val modelDir = "tmp/model"

    val df = sparkSession.createDataFrame(Seq(
      (1, Vectors.dense(0.0, 1.0, 4.0), 1.0),
      (2, Vectors.dense(1.0, 0.0, 4.0), 2.0),
      (3, Vectors.dense(1.0, 0.0, 5.0), 3.0),
      (4, Vectors.dense(0.0, 0.0, 5.0), 4.0)
    )).toDF("id", "features", "label")

    val scaler = new MinMaxScaler()
      .setInputCol("features")
      .setOutputCol("features_scaled")
      .setMin(0.0)
      .setMax(3.0)
    val pipeline = new Pipeline().setStages(Array(scaler))

    val fitStartTime = System.nanoTime()
    val model = pipeline.fit(df)
    val fitEndTime = System.nanoTime()



  }

}
