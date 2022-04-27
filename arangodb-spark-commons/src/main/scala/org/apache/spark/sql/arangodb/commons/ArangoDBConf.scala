package org.apache.spark.sql.arangodb.commons

import com.arangodb.{ArangoDB, entity}
import com.arangodb.model.OverwriteMode
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, DropMalformedMode, FailFastMode, ParseMode, PermissiveMode}

import java.io.ByteArrayInputStream
import java.security.KeyStore
import java.security.cert.CertificateFactory
import java.util
import java.util.Base64
import javax.net.ssl.{SSLContext, TrustManagerFactory}
import scala.collection.JavaConverters.{mapAsJavaMapConverter, mapAsScalaMapConverter}

object ArangoDBConf {

  val USER = "user"
  val userConf: ConfigEntry[String] = ConfigBuilder(USER)
    .doc("db user")
    .stringConf
    .createWithDefault("root")

  val PASSWORD = "password"
  val passwordConf: OptionalConfigEntry[String] = ConfigBuilder(PASSWORD)
    .doc("db password")
    .stringConf
    .createOptional

  val ENDPOINTS = "endpoints"
  val endpointsConf: OptionalConfigEntry[String] = ConfigBuilder(ENDPOINTS)
    .doc("A comma-separated list of coordinators, eg. c1:8529,c2:8529")
    .stringConf
    .createOptional

  val ACQUIRE_HOST_LIST = "acquireHostList"
  val acquireHostListConf: ConfigEntry[Boolean] = ConfigBuilder(ACQUIRE_HOST_LIST)
    .doc("acquire the list of all known hosts in the cluster")
    .booleanConf
    .createWithDefault(false)

  val PROTOCOL = "protocol"
  val protocolConf: ConfigEntry[String] = ConfigBuilder(PROTOCOL)
    .doc("communication protocol")
    .stringConf
    .checkValues(Set(Protocol.HTTP.name, Protocol.VST.name))
    .createWithDefault(Protocol.HTTP.name)

  val CONTENT_TYPE = "contentType"
  val contentTypeConf: ConfigEntry[String] = ConfigBuilder(CONTENT_TYPE)
    .doc("content type for driver communication")
    .stringConf
    .checkValues(Set(ContentType.VPACK.name, ContentType.JSON.name))
    .createWithDefault(ContentType.JSON.name)

  val TIMEOUT = "timeout"
  val DEFAULT_TIMEOUT: Int = 5 * 60 * 1000
  val timeoutConf: ConfigEntry[Int] = ConfigBuilder(TIMEOUT)
    .doc("driver connect and request timeout in ms")
    .intConf
    .createWithDefault(DEFAULT_TIMEOUT)

  val SSL_ENABLED = "ssl.enabled"
  val sslEnabledConf: ConfigEntry[Boolean] = ConfigBuilder(SSL_ENABLED)
    .doc("SSL secured driver connection")
    .booleanConf
    .createWithDefault(false)

  val SSL_CERT_VALUE = "ssl.cert.value"
  val sslCertValueConf: OptionalConfigEntry[String] = ConfigBuilder(SSL_CERT_VALUE)
    .doc("base64 encoded certificate")
    .stringConf
    .createOptional

  val SSL_CERT_TYPE = "ssl.cert.type"
  val sslCertTypeConf: ConfigEntry[String] = ConfigBuilder(SSL_CERT_TYPE)
    .doc("certificate type")
    .stringConf
    .createWithDefault("X.509")

  val SSL_CERT_ALIAS = "ssl.cert.alias"
  val sslCertAliasConf: ConfigEntry[String] = ConfigBuilder(SSL_CERT_ALIAS)
    .doc("certificate alias name")
    .stringConf
    .createWithDefault("arangodb")

  val SSL_ALGORITHM = "ssl.algorithm"
  val sslAlgorithmConf: ConfigEntry[String] = ConfigBuilder(SSL_ALGORITHM)
    .doc("trust manager algorithm")
    .stringConf
    .createWithDefault("SunX509")

  val SSL_KEYSTORE_TYPE = "ssl.keystore.type"
  val sslKeystoreTypeConf: ConfigEntry[String] = ConfigBuilder(SSL_KEYSTORE_TYPE)
    .doc("keystore type")
    .stringConf
    .createWithDefault("jks")

  val SSL_PROTOCOL = "ssl.protocol"
  val sslProtocolConf: ConfigEntry[String] = ConfigBuilder(SSL_PROTOCOL)
    .doc("SSLContext protocol")
    .stringConf
    .createWithDefault("TLS")

  val DB = "database"
  val dbConf: ConfigEntry[String] = ConfigBuilder(DB)
    .doc("database name")
    .stringConf
    .createWithDefault("_system")

  val COLLECTION = "table"
  val collectionConf: OptionalConfigEntry[String] = ConfigBuilder(COLLECTION)
    .doc("ArangoDB collection name")
    .stringConf
    .createOptional

  val BATCH_SIZE = "batchSize"
  val DEFAULT_BATCH_SIZE = 10000
  val batchSizeConf: ConfigEntry[Int] = ConfigBuilder(BATCH_SIZE)
    .doc("batch size")
    .intConf
    .createWithDefault(DEFAULT_BATCH_SIZE)

  val QUERY = "query"
  val queryConf: OptionalConfigEntry[String] = ConfigBuilder(QUERY)
    .doc("custom AQL read query")
    .stringConf
    .createOptional

  val SAMPLE_SIZE = "sampleSize"
  val DEFAULT_SAMPLE_SIZE = 1000
  val sampleSizeConf: ConfigEntry[Int] = ConfigBuilder(SAMPLE_SIZE)
    .doc("sample size prefetched for schema inference")
    .intConf
    .createWithDefault(DEFAULT_SAMPLE_SIZE)

  val FILL_BLOCK_CACHE = "fillBlockCache"
  val fillBlockCacheConf: ConfigEntry[Boolean] = ConfigBuilder(FILL_BLOCK_CACHE)
    .doc("whether the query should store the data it reads in the RocksDB block cache")
    .booleanConf
    .createWithDefault(false)

  val STREAM = "stream"
  val streamConf: ConfigEntry[Boolean] = ConfigBuilder(STREAM)
    .doc("whether the query should be executed lazily")
    .booleanConf
    .createWithDefault(true)

  val PARSE_MODE = "mode"
  val parseModeConf: ConfigEntry[String] = ConfigBuilder(PARSE_MODE)
    .doc("allows a mode for dealing with corrupt records during parsing")
    .stringConf
    .checkValues(Set(PermissiveMode.name, DropMalformedMode.name, FailFastMode.name))
    .createWithDefault(PermissiveMode.name)

  val COLUMN_NAME_OF_CORRUPT_RECORD = "columnNameOfCorruptRecord"
  val columnNameOfCorruptRecordConf: OptionalConfigEntry[String] = ConfigBuilder(COLUMN_NAME_OF_CORRUPT_RECORD)
    .doc("allows renaming the new field having malformed string created by PERMISSIVE mode")
    .stringConf
    .createOptional

  val NUMBER_OF_SHARDS = "table.shards"
  val DEFAULT_NUMBER_OF_SHARDS = 1
  val numberOfShardsConf: ConfigEntry[Int] = ConfigBuilder(NUMBER_OF_SHARDS)
    .doc("number of shards of the created collection (in case of SaveMode Append or Overwrite)")
    .intConf
    .createWithDefault(DEFAULT_NUMBER_OF_SHARDS)

  val COLLECTION_TYPE = "table.type"
  val collectionTypeConf: ConfigEntry[String] = ConfigBuilder(COLLECTION_TYPE)
    .doc("type of the created collection (in case of SaveMode Append or Overwrite)")
    .stringConf
    .checkValues(Set(CollectionType.DOCUMENT.name, CollectionType.EDGE.name))
    .createWithDefault(CollectionType.DOCUMENT.name)

  val WAIT_FOR_SYNC = "waitForSync"
  val waitForSyncConf: ConfigEntry[Boolean] = ConfigBuilder(WAIT_FOR_SYNC)
    .doc("whether to wait until the documents have been synced to disk")
    .booleanConf
    .createWithDefault(false)

  val CONFIRM_TRUNCATE = "confirmTruncate"
  val confirmTruncateConf: ConfigEntry[Boolean] = ConfigBuilder(CONFIRM_TRUNCATE)
    .doc("confirm to truncate table when using SaveMode.Overwrite mode")
    .booleanConf
    .createWithDefault(false)

  val OVERWRITE_MODE = "overwriteMode"
  val overwriteModeConf: ConfigEntry[String] = ConfigBuilder(OVERWRITE_MODE)
    .doc("configures the behavior in case a document with the specified _key value exists already")
    .stringConf
    .checkValues(Set(
      OverwriteMode.ignore.getValue,
      OverwriteMode.replace.getValue,
      OverwriteMode.update.getValue,
      OverwriteMode.conflict.getValue
    ))
    .createWithDefault(OverwriteMode.conflict.getValue)

  val MERGE_OBJECTS = "mergeObjects"
  val mergeObjectsConf: ConfigEntry[Boolean] = ConfigBuilder(MERGE_OBJECTS)
    .doc("in case overwrite.mode is set to update, controls whether objects (not arrays) will be merged")
    .booleanConf
    .createWithDefault(true)

  val KEEP_NULL = "keepNull"
  val keepNullConf: ConfigEntry[Boolean] = ConfigBuilder(KEEP_NULL)
    .doc("whether null values are saved within the document or used to delete corresponding existing attributes")
    .booleanConf
    .createWithDefault(true)

  val MAX_ATTEMPTS = "retry.maxAttempts"
  val DEFAULT_MAX_ATTEMPTS = 10
  val maxAttemptsConf: ConfigEntry[Int] = ConfigBuilder(MAX_ATTEMPTS)
    .doc("max attempts for write requests, in case they can be retried")
    .intConf
    .createWithDefault(DEFAULT_MAX_ATTEMPTS)

  val MIN_RETRY_DELAY = "retry.minDelay"
  val DEFAULT_MIN_RETRY_DELAY = 0
  val minRetryDelayConf: ConfigEntry[Int] = ConfigBuilder(MIN_RETRY_DELAY)
    .doc("min delay in ms between write requests retries")
    .intConf
    .createWithDefault(DEFAULT_MIN_RETRY_DELAY)

  val MAX_RETRY_DELAY = "retry.maxDelay"
  val DEFAULT_MAX_RETRY_DELAY = 0
  val maxRetryDelayConf: ConfigEntry[Int] = ConfigBuilder(MAX_RETRY_DELAY)
    .doc("max delay in ms between write requests retries")
    .intConf
    .createWithDefault(DEFAULT_MAX_RETRY_DELAY)

  val IGNORE_NULL_FIELDS = "ignoreNullFields"
  val ignoreNullFieldsConf: ConfigEntry[Boolean] = ConfigBuilder(IGNORE_NULL_FIELDS)
    .doc("whether to ignore null fields during serialization")
    .booleanConf
    .createWithDefault(false)

  private[sql] val confEntries: Map[String, ConfigEntry[_]] = CaseInsensitiveMap(Map(
    // driver config
    USER -> userConf,
    PASSWORD -> passwordConf,
    ENDPOINTS -> endpointsConf,
    ACQUIRE_HOST_LIST -> acquireHostListConf,
    PROTOCOL -> protocolConf,
    CONTENT_TYPE -> contentTypeConf,
    TIMEOUT -> timeoutConf,
    SSL_ENABLED -> sslEnabledConf,
    SSL_CERT_VALUE -> sslCertValueConf,
    SSL_CERT_TYPE -> sslCertTypeConf,
    SSL_CERT_ALIAS -> sslCertAliasConf,
    SSL_ALGORITHM -> sslAlgorithmConf,
    SSL_KEYSTORE_TYPE -> sslKeystoreTypeConf,
    SSL_PROTOCOL -> sslProtocolConf,

    // read/write config
    DB -> dbConf,
    COLLECTION -> collectionConf,
    BATCH_SIZE -> batchSizeConf,

    // read config
    QUERY -> queryConf,
    SAMPLE_SIZE -> sampleSizeConf,
    FILL_BLOCK_CACHE -> fillBlockCacheConf,
    STREAM -> streamConf,
    PARSE_MODE -> parseModeConf,
    COLUMN_NAME_OF_CORRUPT_RECORD -> columnNameOfCorruptRecordConf,

    // write config
    NUMBER_OF_SHARDS -> numberOfShardsConf,
    COLLECTION_TYPE -> collectionTypeConf,
    WAIT_FOR_SYNC -> waitForSyncConf,
    CONFIRM_TRUNCATE -> confirmTruncateConf,
    OVERWRITE_MODE -> overwriteModeConf,
    MERGE_OBJECTS -> mergeObjectsConf,
    KEEP_NULL -> keepNullConf,
    MAX_ATTEMPTS -> maxAttemptsConf,
    MIN_RETRY_DELAY -> minRetryDelayConf,
    MAX_RETRY_DELAY -> maxRetryDelayConf,
    IGNORE_NULL_FIELDS -> ignoreNullFieldsConf
  ))

  /**
   * Holds information about keys that have been deprecated.
   *
   * @param key     The deprecated key.
   * @param version Version of Spark ArangoDB where key was deprecated.
   * @param comment Additional info regarding to the removed config. For example,
   *                reasons of config deprecation, what users should use instead of it.
   */
  case class DeprecatedConfig(key: String, version: String, comment: String)

  /**
   * Maps deprecated Spark ArangoDB config keys to information about the deprecation.
   *
   * The extra information is logged as a warning when the Spark ArangoDB config is present
   * in the user's configuration.
   */
  val deprecatedArangoDBConfigs: Map[String, DeprecatedConfig] = {
    val configs: Seq[DeprecatedConfig] = Seq()
    CaseInsensitiveMap(Map(configs.map { cfg => cfg.key -> cfg }: _*))
  }

  /**
   * Holds information about keys that have been removed.
   *
   * @param key          The removed config key.
   * @param version      Version of Spark ArangoDB where key was removed.
   * @param defaultValue The default config value. It can be used to notice
   *                     users that they set non-default value to an already removed config.
   * @param comment      Additional info regarding to the removed config.
   */
  case class RemovedConfig(key: String, version: String, defaultValue: String, comment: String)

  /**
   * The map contains info about removed Spark ArangoDB configs. Keys are Spark ArangoDB config names,
   * map values contain extra information like the version in which the config was removed,
   * config's default value and a comment.
   */
  val removedArangoDBConfigs: Map[String, RemovedConfig] = {
    val configs: Seq[RemovedConfig] = Seq()
    CaseInsensitiveMap(Map(configs.map { cfg => cfg.key -> cfg }: _*))
  }

  def apply(options: Map[String, String]): ArangoDBConf = new ArangoDBConf(options)

  def apply(options: util.Map[String, String]): ArangoDBConf = ArangoDBConf(options.asScala.toMap)

}

class ArangoDBConf(opts: Map[String, String]) extends Serializable with Logging {

  import ArangoDBConf._

  private val settings = CaseInsensitiveMap(opts)
  settings.foreach(i => checkConf(i._1, i._2))

  @transient protected val reader = new ConfigReader(settings.asJava)

  lazy val driverOptions: ArangoDBDriverConf = new ArangoDBDriverConf(settings)
  lazy val readOptions: ArangoDBReadConf = new ArangoDBReadConf(settings)
  lazy val writeOptions: ArangoDBWriteConf = new ArangoDBWriteConf(settings)

  def updated(kv: (String, String)): ArangoDBConf = new ArangoDBConf(settings + kv)

  def updated(other: ArangoDBConf): ArangoDBConf = new ArangoDBConf(settings ++ other.settings)

  protected def getRequiredConf[T](entry: OptionalConfigEntry[T]): T =
    getConf(entry).getOrElse(throw new IllegalArgumentException(s"Required ${entry.key} configuration parameter"))

  /** Return the value of Spark ArangoDB configuration property for the given key. */
  @throws[NoSuchElementException]("if key is not set")
  def getConfString(key: String): String = settings.get(key).getOrElse(throw new NoSuchElementException(key))

  /**
   * Return the value of Spark ArangoDB configuration property for the given key. If the key is not set
   * yet, return `defaultValue`. This is useful when `defaultValue` in ConfigEntry is not the
   * desired one.
   */
  def getConf[T](entry: ConfigEntry[T], defaultValue: T): T =
    settings.get(entry.key).map(entry.valueConverter).getOrElse(defaultValue)

  /**
   * Return the value of Spark ArangoDB configuration property for the given key. If the key is not set
   * yet, return `defaultValue` in [[ConfigEntry]].
   */
  protected def getConf[T](entry: ConfigEntry[T]): T = entry.readFrom(reader)

  /**
   * Return the value of an optional Spark ArangoDB configuration property for the given key. If the key
   * is not set yet, returns None.
   */
  protected def getConf[T](entry: OptionalConfigEntry[T]): Option[T] = entry.readFrom(reader)

  /**
   * Return the `string` value of Spark ArangoDB configuration property for the given key. If the key is
   * not set, return `defaultValue`.
   */
  def getConfString(key: String, defaultValue: String): String = settings.get(key).getOrElse(defaultValue)

  /**
   * Return all the configuration properties that have been set (i.e. not the default).
   * This creates a new copy of the config properties in the form of a Map.
   */
  def getAllConfigs: Map[String, String] = settings.toMap

  /**
   * Return all the configuration properties that are effective (either set or default).
   * This creates a new copy of the config properties in the form of a Map.
   */
  def getAllEffectiveConfigs: Map[String, String] = getAllDefinedConfigs.map(entry => (entry._1, entry._2)).toMap

  /**
   * Return all the configuration definitions that have been defined in [[ArangoDBConf]]. Each
   * definition contains key, defaultValue and doc.
   */
  def getAllDefinedConfigs: Seq[(String, String, String)] =
    confEntries.values.filter(_.isPublic).map { entry =>
      val displayValue = settings.get(entry.key).getOrElse(entry.defaultValueString)
      (entry.key, displayValue, entry.doc)
    }.toSeq

  /**
   * Logs a warning message if the given config key is deprecated.
   */
  private def logDeprecationWarning(key: String): Unit = {
    ArangoDBConf.deprecatedArangoDBConfigs.get(key).foreach {
      case DeprecatedConfig(configName, version, comment) =>
        logWarning(
          s"The Spark ArangoDB config '$configName' has been deprecated since version v$version " +
            s"and may be removed in the future. $comment")
    }
  }

  private def requireDefaultValueOfRemovedConf(key: String, value: String): Unit = {
    ArangoDBConf.removedArangoDBConfigs.get(key).foreach {
      case RemovedConfig(configName, version, defaultValue, comment) =>
        if (value != defaultValue) {
          throw new AnalysisException(
            s"The Spark ArangoDB config '$configName' was removed in the version $version. $comment")
        }
    }
  }

  private def checkConf(key: String, value: String): Unit = {
    logDeprecationWarning(key)
    requireDefaultValueOfRemovedConf(key, value)
  }

}


class ArangoDBDriverConf(opts: Map[String, String]) extends ArangoDBConf(opts) {

  import ArangoDBConf._

  val user: String = getConf(userConf)

  val password: Option[String] = getConf(passwordConf)

  val endpoints: Array[String] = getRequiredConf(endpointsConf).split(",")

  val acquireHostList: Boolean = getConf(acquireHostListConf)

  val contentType: ContentType = ContentType(getConf(contentTypeConf))

  val timeout: Int = getConf(timeoutConf)

  private val arangoProtocol = (Protocol(getConf(protocolConf)), contentType) match {
    case (Protocol.VST, ContentType.VPACK) => com.arangodb.Protocol.VST
    case (Protocol.VST, ContentType.JSON) => throw new IllegalArgumentException("Json over VST is not supported")
    case (Protocol.HTTP, ContentType.VPACK) => com.arangodb.Protocol.HTTP_VPACK
    case (Protocol.HTTP, ContentType.JSON) => com.arangodb.Protocol.HTTP_JSON
  }

  val sslEnabled: Boolean = getConf(sslEnabledConf)

  val sslCertValue: Option[String] = getConf(sslCertValueConf)

  val sslCertType: String = getConf(sslCertTypeConf)

  val sslCertAlias: String = getConf(sslCertAliasConf)

  val sslAlgorithm: String = getConf(sslAlgorithmConf)

  val sslKeystoreType: String = getConf(sslKeystoreTypeConf)

  val sslProtocol: String = getConf(sslProtocolConf)

  def builder(): ArangoDB.Builder = {
    val builder = new ArangoDB.Builder()
      .useProtocol(arangoProtocol)
      .timeout(timeout)
      .user(user)
      .maxConnections(1)
    password.foreach(builder.password)

    if (sslEnabled) {
      builder
        .useSsl(true)
        .sslContext(getSslContext)
    }

    endpoints
      .map(_.split(":"))
      .foreach(host => builder.host(host(0), host(1).toInt))
    builder
  }

  def getSslContext: SSLContext = sslCertValue match {
    case Some(b64cert) =>
      val is = new ByteArrayInputStream(Base64.getDecoder.decode(b64cert))
      val cert = CertificateFactory.getInstance(sslCertType).generateCertificate(is)
      val ks = KeyStore.getInstance(sslKeystoreType)
      ks.load(null) // scalastyle:ignore null
      ks.setCertificateEntry(sslCertAlias, cert)
      val tmf = TrustManagerFactory.getInstance(sslAlgorithm)
      tmf.init(ks)
      val sc = SSLContext.getInstance(sslProtocol)
      sc.init(null, tmf.getTrustManagers, null) // scalastyle:ignore null
      sc
    case None => SSLContext.getDefault
  }


}


class ArangoDBReadConf(opts: Map[String, String]) extends ArangoDBConf(opts) {

  import ArangoDBConf._

  val db: String = getConf(dbConf)

  val collection: Option[String] = getConf(collectionConf)

  val query: Option[String] = getConf(queryConf)

  val batchSize: Int = getConf(batchSizeConf)

  val sampleSize: Int = getConf(sampleSizeConf)

  val fillBlockCache: Boolean = getConf(fillBlockCacheConf)

  val stream: Boolean = getConf(streamConf)

  val parseMode: ParseMode = ParseMode.fromString(getConf(parseModeConf))

  val columnNameOfCorruptRecord: String = getConf(columnNameOfCorruptRecordConf).getOrElse("")

  val readMode: ReadMode =
    if (query.isDefined) {
      ReadMode.Query
    } else if (collection.isDefined) {
      ReadMode.Collection
    } else {
      throw new IllegalArgumentException("Either collection or query must be defined")
    }

}


class ArangoDBWriteConf(opts: Map[String, String]) extends ArangoDBConf(opts) {

  import ArangoDBConf._

  val db: String = getConf(dbConf)

  val collection: String = getRequiredConf(collectionConf)

  val batchSize: Int = getConf(batchSizeConf)

  val numberOfShards: Int = getConf(numberOfShardsConf)

  val collectionType: entity.CollectionType = CollectionType(getConf(collectionTypeConf)).get()

  val waitForSync: Boolean = getConf(waitForSyncConf)

  val confirmTruncate: Boolean = getConf(confirmTruncateConf)

  val overwriteMode: OverwriteMode = OverwriteMode.valueOf(getConf(overwriteModeConf))

  val mergeObjects: Boolean = getConf(mergeObjectsConf)

  val keepNull: Boolean = getConf(keepNullConf)

  val maxAttempts: Int = getConf(maxAttemptsConf)

  val minRetryDelay: Int = getConf(minRetryDelayConf)

  val maxRetryDelay: Int = getConf(maxRetryDelayConf)

  val ignoreNullFields: Boolean = getConf(ignoreNullFieldsConf)

  override def toString =
    s"""ArangoDBWriteConf(
       |\t db=$db
       |\t collection=$collection
       |\t batchSize=$batchSize
       |\t numberOfShards=$numberOfShards
       |\t collectionType=$collectionType
       |\t waitForSync=$waitForSync
       |\t confirmTruncate=$confirmTruncate
       |\t overwriteMode=$overwriteMode
       |\t mergeObjects=$mergeObjects
       |\t keepNull=$keepNull
       |\t maxAttempts=$maxAttempts
       |\t minRetryDelay=$minRetryDelay
       |\t maxRetryDelay=$maxRetryDelay
       |\t ignoreNullFields=$ignoreNullFields
       |)""".stripMargin
}
