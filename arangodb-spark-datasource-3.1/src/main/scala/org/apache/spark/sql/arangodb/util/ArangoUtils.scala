/*
 * DISCLAIMER
 *
 * Copyright 2016 ArangoDB GmbH, Cologne, Germany
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Copyright holder is ArangoDB GmbH, Cologne, Germany
 */

package org.apache.spark.sql.arangodb.util

import org.apache.spark.sql.arangodb.datasource.{ArangoOptions, ReadMode}
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
