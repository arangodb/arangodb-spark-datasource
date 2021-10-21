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
import com.arangodb.model.OverwriteMode

import java.io.ByteArrayInputStream
import java.security.KeyStore
import java.security.cert.CertificateFactory
import java.util
import java.util.Base64
import javax.net.ssl.{SSLContext, TrustManagerFactory}
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

  val SSL_ENABLED = "ssl.enabled"

  // Base64 encoded certificate
  val SSL_CERT = "ssl.cert.value"

  // certificate type, default "X.509"
  val SSL_CERT_TYPE = "ssl.cert.type"

  // certificate alias name
  val SSL_CERT_ALIAS = "ssl.cert.alias"

  // trustmanager algorithm, default "SunX509"
  val SSL_ALGORITHM = "ssl.algorithm"

  // keystore type, default "jks"
  val SSL_KEYSTORE = "ssl.keystore.type"

  // SSLContext protocol, default "TLS"
  val SSL_PROTOCOL = "ssl.protocol"

  // read/write options
  val DB = "database"
  val COLLECTION = "table"
  val BATCH_SIZE = "batch.size"
  val CONTENT_TYPE = "content-type"
  val ACQUIRE_HOST_LIST = "acquire-host-list"

  // read options
  val QUERY = "query"
  val SAMPLE_SIZE = "sample.size"
  val FILL_BLOCK_CACHE = "fill.cache"
  val STREAM = "stream"

  // write options
  val WAIT_FOR_SYNC = "wait.sync"
  val CONFIRM_TRUNCATE = "confirm.truncate"
  val OVERWRITE_MODE = "overwrite.mode"
  val MERGE_OBJECTS = "merge.objects"

  def apply(options: Map[String, String]): ArangoOptions = new ArangoOptions(options)

  def apply(options: util.Map[String, String]): ArangoOptions = ArangoOptions(options.asScala.toMap)

}

class ArangoDriverOptions(options: Map[String, String]) extends Serializable {
  private val protocol = Protocol(options.getOrElse(ArangoOptions.PROTOCOL, "http"))
  private val contentType: ContentType = ContentType(options.getOrElse(ArangoOptions.CONTENT_TYPE, "vpack"))
  private val arangoProtocol = (protocol, contentType) match {
    case (Protocol.VST, ContentType.VPack) => com.arangodb.Protocol.VST
    case (Protocol.VST, ContentType.Json) => throw new IllegalArgumentException("Json over VST is not supported")
    case (Protocol.HTTP, ContentType.VPack) => com.arangodb.Protocol.HTTP_VPACK
    case (Protocol.HTTP, ContentType.Json) => com.arangodb.Protocol.HTTP_JSON
  }
  private val sslEnabled: Boolean = options.getOrElse(ArangoOptions.SSL_ENABLED, "false").toBoolean
  private val sslCert: Option[String] = options.get(ArangoOptions.SSL_CERT)
  private val sslCertType: String = options.getOrElse(ArangoOptions.SSL_CERT_TYPE, "X.509")
  private val sslCertAlias: String = options.getOrElse(ArangoOptions.SSL_CERT_ALIAS, "arangodb")
  private val sslAlgorithm: String = options.getOrElse(ArangoOptions.SSL_ALGORITHM, TrustManagerFactory.getDefaultAlgorithm)
  private val sslKeystore: String = options.getOrElse(ArangoOptions.SSL_KEYSTORE, KeyStore.getDefaultType)
  private val sslProtocol: String = options.getOrElse(ArangoOptions.SSL_PROTOCOL, "TLS")

  val endpoints: Seq[String] = options(ArangoOptions.ENDPOINTS).split(",")
  val acquireHostList: Boolean = options.getOrElse(ArangoOptions.ACQUIRE_HOST_LIST, "false").toBoolean

  def builder(): ArangoDB.Builder = {
    val builder = new ArangoDB.Builder()
      .useProtocol(arangoProtocol)

    if (sslEnabled) {
      builder
        .useSsl(true)
        .sslContext(getSslContext)
    }

    options.get(ArangoOptions.USER).foreach(builder.user)
    options.get(ArangoOptions.PASSWORD).foreach(builder.password)
    endpoints
      .map(_.split(":"))
      .foreach(host => builder.host(host(0), host(1).toInt))
    builder
  }

  def getSslContext: SSLContext = sslCert match {
    case Some(b64cert) =>
      val is = new ByteArrayInputStream(Base64.getDecoder.decode(b64cert))
      val cert = CertificateFactory.getInstance(sslCertType).generateCertificate(is)
      val ks = KeyStore.getInstance(sslKeystore)
      ks.load(null)
      ks.setCertificateEntry(sslCertAlias, cert)
      val tmf = TrustManagerFactory.getInstance(sslAlgorithm)
      tmf.init(ks)
      val sc = SSLContext.getInstance(sslProtocol)
      sc.init(null, tmf.getTrustManagers, null)
      sc
    case None => SSLContext.getDefault
  }

}

abstract class CommonOptions(options: Map[String, String]) extends Serializable {
  val db: String = options.getOrElse(ArangoOptions.DB, "_system")
  val contentType: ContentType = ContentType(options.getOrElse(ArangoOptions.CONTENT_TYPE, "vpack"))

  protected def getRequired(key: String): String = options
    .getOrElse(key, throw new IllegalArgumentException(s"Required $key configuration parameter not found"))
}

class ArangoReadOptions(options: Map[String, String]) extends CommonOptions(options) {
  val batchSize: Option[Int] = options.get(ArangoOptions.BATCH_SIZE).map(_.toInt)
  val sampleSize: Int = options.get(ArangoOptions.SAMPLE_SIZE).map(_.toInt).getOrElse(1000)
  val collection: Option[String] = options.get(ArangoOptions.COLLECTION)
  val query: Option[String] = options.get(ArangoOptions.QUERY)
  val readMode: ReadMode =
    if (query.isDefined) ReadMode.Query
    else if (collection.isDefined) ReadMode.Collection
    else throw new IllegalArgumentException("Either collection or query must be defined")
  val fillBlockCache: Option[Boolean] = options.get(ArangoOptions.FILL_BLOCK_CACHE).map(_.toBoolean)
  val stream: Boolean = options.getOrElse(ArangoOptions.STREAM, "true").toBoolean
}

class ArangoWriteOptions(options: Map[String, String]) extends CommonOptions(options) {
  val batchSize: Int = options.get(ArangoOptions.BATCH_SIZE).map(_.toInt).getOrElse(1000)
  val collection: String = getRequired(ArangoOptions.COLLECTION)
  val waitForSync: Option[Boolean] = options.get(ArangoOptions.WAIT_FOR_SYNC).map(_.toBoolean)
  val confirmTruncate: Boolean = options.getOrElse(ArangoOptions.CONFIRM_TRUNCATE, "false").toBoolean
  val overwriteMode: Option[OverwriteMode] = options.get(ArangoOptions.OVERWRITE_MODE).map(OverwriteMode.valueOf)
  val mergeObjects: Option[Boolean] = options.get(ArangoOptions.MERGE_OBJECTS).map(_.toBoolean)
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
