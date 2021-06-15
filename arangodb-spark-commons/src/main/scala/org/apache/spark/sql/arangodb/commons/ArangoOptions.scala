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

package org.apache.spark.sql.arangodb.commons

import com.arangodb.ArangoDB

import java.util
import scala.collection.JavaConverters.mapAsScalaMapConverter


/**
 * @author Michele Rastelli
 */
class ArangoOptions(private val options: Map[String, String]) extends Serializable {
  lazy val driverOptions: ArangoDriverOptions = new ArangoDriverOptions(options)
  lazy val readOptions: ArangoReadOptions = new ArangoReadOptions(options)
  lazy val writeOptions: ArangoWriteOptions = new ArangoWriteOptions(options)

  def updated(kv: (String, String)): ArangoOptions = new ArangoOptions(options + kv)

  def updated(other: ArangoOptions): ArangoOptions = new ArangoOptions(options ++ other.options)
}

object ArangoOptions {

  // driver options
  val USER = "user"
  val PASSWORD = "password"
  val ENDPOINTS = "endpoints"
  val PROTOCOL = "protocol"

  // read/write options
  val DB = "database"
  val COLLECTION = "table"
  val QUERY = "query"
  val SAMPLE_SIZE = "sample.size"
  val BATCH_SIZE = "batch.size"
  val CONTENT_TYPE = "content-type"
  val TOPOLOGY = "topology"

  def apply(options: Map[String, String]): ArangoOptions = new ArangoOptions(options)

  def apply(options: util.Map[String, String]): ArangoOptions = ArangoOptions(options.asScala.toMap)

}

class ArangoDriverOptions(options: Map[String, String]) extends Serializable {
  private val protocol = Protocol(options.getOrElse(ArangoOptions.PROTOCOL, "vst"))
  private val contentType: ContentType = ContentType(options.getOrElse(ArangoOptions.CONTENT_TYPE, "vpack"))
  private val arangoProtocol = (protocol, contentType) match {
    case (Protocol.VST, ContentType.VPack) => com.arangodb.Protocol.VST
    case (Protocol.VST, ContentType.Json) => throw new IllegalArgumentException("Json over VST is not supported")
    case (Protocol.HTTP, ContentType.VPack) => com.arangodb.Protocol.HTTP_VPACK
    case (Protocol.HTTP, ContentType.Json) => com.arangodb.Protocol.HTTP_JSON
  }

  def builder(): ArangoDB.Builder = {
    val builder = new ArangoDB.Builder()
      .useProtocol(arangoProtocol)
    options.get(ArangoOptions.USER).foreach(builder.user)
    options.get(ArangoOptions.PASSWORD).foreach(builder.password)
    endpoints
      .map(_.split(":"))
      .foreach(host => builder.host(host(0), host(1).toInt))
    builder
  }

  def endpoints: Seq[String] = options
    .get(ArangoOptions.ENDPOINTS).toList
    .flatMap(_.split(","))
}

abstract class CommonOptions(options: Map[String, String]) extends Serializable {
  val db: String = options.getOrElse(ArangoOptions.DB, "_system")
  val batchSize: Int = options.get(ArangoOptions.BATCH_SIZE).map(_.toInt).getOrElse(1000)
  val contentType: ContentType = ContentType(options.getOrElse(ArangoOptions.CONTENT_TYPE, "vpack"))

  protected def getRequired(key: String): String = options
    .getOrElse(key, throw new IllegalArgumentException(s"Required $key configuration parameter not found"))
}

class ArangoReadOptions(options: Map[String, String]) extends CommonOptions(options) {
  val sampleSize: Int = options.get(ArangoOptions.SAMPLE_SIZE).map(_.toInt).getOrElse(1000)
  val collection: Option[String] = options.get(ArangoOptions.COLLECTION)
  val query: Option[String] = options.get(ArangoOptions.QUERY)
  val readMode: ReadMode = {
    if (query.isDefined) ReadMode.Query
    else if (collection.isDefined) ReadMode.Collection
    else throw new IllegalArgumentException("Either collection or query must be defined")
  }
  val arangoTopology: ArangoTopology = ArangoTopology(options.getOrElse(ArangoOptions.TOPOLOGY, "cluster"))
}

class ArangoWriteOptions(options: Map[String, String]) extends CommonOptions(options) {
  val collection: String = getRequired(ArangoOptions.COLLECTION)
}

sealed trait ReadMode

object ReadMode {
  /**
   * Read from an Arango collection. The scan will be partitioned according to the collection shards.
   */
  case object Collection extends ReadMode

  /**
   * Read executing a user query, without partitioning.
   */
  case object Query extends ReadMode
}

sealed trait ContentType

object ContentType {
  case object Json extends ContentType

  case object VPack extends ContentType

  def apply(value: String): ContentType = value match {
    case "json" => Json
    case "vpack" => VPack
    case _ => throw new IllegalArgumentException(s"${ArangoOptions.CONTENT_TYPE}: $value")
  }
}

sealed trait Protocol

object Protocol {
  case object VST extends Protocol

  case object HTTP extends Protocol

  def apply(value: String): Protocol = value match {
    case "vst" => VST
    case "http" => HTTP
    case _ => throw new IllegalArgumentException(s"${ArangoOptions.PROTOCOL}: $value")
  }
}

sealed trait ArangoTopology

object ArangoTopology {
  case object SINGLE extends ArangoTopology

  case object CLUSTER extends ArangoTopology

  def apply(value: String): ArangoTopology = value match {
    case "single" => SINGLE
    case "cluster" => CLUSTER
    case _ => throw new IllegalArgumentException(s"${ArangoOptions.TOPOLOGY}: $value")
  }
}
