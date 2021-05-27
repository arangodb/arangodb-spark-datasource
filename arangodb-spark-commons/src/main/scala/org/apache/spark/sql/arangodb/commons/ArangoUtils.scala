package org.apache.spark.sql.arangodb.commons

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Encoders, SparkSession}

/**
 * @author Michele Rastelli
 */
object ArangoUtils {

  def inferSchema(options: ArangoOptions): StructType = {
    val client = ArangoClient(options)
    val sampleEntries = options.readOptions.readMode match {
      case ReadMode.Query => client.readQuerySample()
      case ReadMode.Collection => client.readCollectionSample()
    }
    client.shutdown()

    val spark = SparkSession.getActiveSession.get
    spark
      .read
      .json(spark.createDataset(sampleEntries)(Encoders.STRING))
      .schema
  }

}
