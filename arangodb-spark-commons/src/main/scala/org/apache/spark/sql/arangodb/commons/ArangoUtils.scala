package org.apache.spark.sql.arangodb.commons

import org.apache.spark.sql.types.{StringType, StructField, StructType}
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
    val schema = spark
      .read
      .json(spark.createDataset(sampleEntries)(Encoders.STRING))
      .schema

    if (options.readOptions.columnNameOfCorruptRecord.isEmpty)
      schema
    else
      schema.add(StructField(options.readOptions.columnNameOfCorruptRecord, StringType, nullable = true))
  }

}
