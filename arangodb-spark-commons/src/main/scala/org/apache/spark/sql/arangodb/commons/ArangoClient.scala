package org.apache.spark.sql.arangodb.commons

import com.arangodb.{ArangoCursor, ArangoDB, ArangoDBException, Request}
import com.arangodb.entity.ErrorEntity
import com.arangodb.internal.serde.{InternalSerde, InternalSerdeProvider}
import com.arangodb.model.{AqlQueryOptions, CollectionCreateOptions}
import com.arangodb.util.{RawBytes, RawJson}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.arangodb.commons.exceptions.ArangoDBMultiException
import org.apache.spark.sql.arangodb.commons.filter.PushableFilter
import org.apache.spark.sql.types.StructType

import java.util.UUID
import java.util.concurrent.TimeoutException
import scala.annotation.tailrec
import scala.collection.JavaConverters.mapAsJavaMapConverter

@SuppressWarnings(Array("OptionGet"))
class ArangoClient(options: ArangoDBConf) extends Logging {

  private def aqlOptions(): AqlQueryOptions = {
    val opt = new AqlQueryOptions()
      .stream(options.readOptions.stream)
      .fillBlockCache(options.readOptions.fillBlockCache)
      .batchSize(options.readOptions.batchSize)
    opt
  }

  lazy val arangoDB: ArangoDB = {
    val serde = new InternalSerdeProvider(options.driverOptions.contentType match {
      case ContentType.JSON => com.arangodb.ContentType.JSON
      case ContentType.VPACK => com.arangodb.ContentType.VPACK
    }).create()
    options.driverOptions
      .builder()
      .serde(serde)
      .build()
  }

  lazy val serde: InternalSerde = arangoDB.getSerde

  def shutdown(): Unit = {
    logDebug("closing db client")
    arangoDB.shutdown()
  }

  def readCollectionPartition(shardId: String, filters: Array[PushableFilter], schema: StructType): ArangoCursor[RawBytes] = {
    val query =
      s"""
         |FOR d IN @@col
         |${PushdownUtils.generateFilterClause(filters)}
         |RETURN ${PushdownUtils.generateColumnsFilter(schema, "d")}"""
        .stripMargin
        .replaceAll("\n", " ")
    val params = Map[String, AnyRef]("@col" -> options.readOptions.collection.get)
    val opts = aqlOptions().shardIds(shardId)
    logDebug(s"""Executing AQL query: \n\t$query ${if (params.nonEmpty) s"\n\t with params: $params" else ""}""")
    arangoDB
      .db(options.readOptions.db)
      .query(query, classOf[RawBytes], params.asJava, opts)
  }

  def readQuery(): ArangoCursor[RawBytes] = {
    val query = options.readOptions.query.get
    logDebug(s"Executing AQL query: \n\t$query")
    arangoDB
      .db(options.readOptions.db)
      .query(
        query,
        classOf[RawBytes],
        aqlOptions())
  }

  def readCollectionSample(): Seq[String] = {
    val query = "FOR d IN @@col LIMIT @size RETURN d"
    val params: Map[String, AnyRef] = Map(
      "@col" -> options.readOptions.collection.get,
      "size" -> (options.readOptions.sampleSize: java.lang.Integer)
    )
    val opts = aqlOptions()
    logDebug(s"""Executing AQL query: \n\t$query ${if (params.nonEmpty) s"\n\t with params: $params" else ""}""")

    import scala.collection.JavaConverters.iterableAsScalaIterableConverter
    arangoDB
      .db(options.readOptions.db)
      .query(query, classOf[RawJson], params.asJava, opts)
      .asListRemaining()
      .asScala
      .toSeq
      .map(_.get)
  }

  def readQuerySample(): Seq[String] = {
    val query = options.readOptions.query.get
    logDebug(s"Executing AQL query: \n\t$query")
    val cursor = arangoDB
      .db(options.readOptions.db)
      .query(
        query,
        classOf[RawJson],
        aqlOptions())

    import scala.collection.JavaConverters.asScalaIteratorConverter
    cursor.asScala.take(options.readOptions.sampleSize).toSeq.map(_.get)
  }

  def collectionExists(): Boolean = {
    logDebug("checking collection")
    arangoDB
      .db(options.writeOptions.db)
      .collection(options.writeOptions.collection)
      .exists()
  }

  def createCollection(): Unit = {
    logDebug("creating collection")
    val opts = new CollectionCreateOptions()
      .numberOfShards(options.writeOptions.numberOfShards)
      .`type`(options.writeOptions.collectionType)

    arangoDB
      .db(options.writeOptions.db)
      .collection(options.writeOptions.collection)
      .create(opts)
  }

  @tailrec
  final def truncate(): Unit = {
    logDebug("truncating collection")
    try {
      arangoDB
        .db(options.writeOptions.db)
        .collection(options.writeOptions.collection)
        .truncate()
      logDebug("truncated collection")
    } catch {
      case e: ArangoDBException =>
        if (e.getCause.isInstanceOf[TimeoutException]) {
          logWarning("Got TimeoutException while truncating collection, retrying...")
          truncate()
        } else {
          throw e
        }
      case t: Throwable => throw t
    }
  }

  def drop(): Unit = {
    logDebug("dropping collection")
    arangoDB
      .db(options.writeOptions.db)
      .collection(options.writeOptions.collection)
      .drop()
  }

  def saveDocuments(data: RawBytes): Unit = {
    logDebug("saving batch")
    val request = new Request.Builder[RawBytes]
      .db(options.writeOptions.db)
      .method(Request.Method.POST)
      .path(s"/_api/document/${options.writeOptions.collection}")
      .queryParam("waitForSync", options.writeOptions.waitForSync.toString)
      .queryParam("overwriteMode", options.writeOptions.overwriteMode.getValue)
      .queryParam("keepNull", options.writeOptions.keepNull.toString)
      .queryParam("mergeObjects", options.writeOptions.mergeObjects.toString)
      .header("x-arango-spark-request-id", UUID.randomUUID.toString)
      .body(RawBytes.of(data.get))
      .build()

    // FIXME: atm silent=true cannot be used due to:
    // - https://arangodb.atlassian.net/browse/BTS-592
    // - https://arangodb.atlassian.net/browse/BTS-816
    // request.putQueryParam("silent", true)

    val response = arangoDB.execute(request, classOf[RawBytes])

    import scala.collection.JavaConverters.asScalaIteratorConverter
    val errors = serde.parse(response.getBody.get).iterator().asScala
      .zip(serde.parse(data.get).iterator().asScala)
      .filter(_._1.has("error"))
      .filter(_._1.get("error").booleanValue())
      .map(it => (
        serde.deserialize[ErrorEntity](it._1, classOf[ErrorEntity]),
        it._2.toString
      ))
      .toArray
    if (errors.nonEmpty) {
      throw new ArangoDBMultiException(errors)
    }
  }
}


@SuppressWarnings(Array("OptionGet"))
object ArangoClient extends Logging {
  private val INTERNAL_ERROR_CODE = 4
  private val SHARDS_API_UNAVAILABLE_CODE = 9

  def apply(options: ArangoDBConf): ArangoClient = {
    logDebug("creating db client")
    new ArangoClient(options)
  }

  def getCollectionShardIds(options: ArangoDBConf): Array[String] = {
    logDebug("reading collection shards")
    val client = ArangoClient(options)
    val adb = client.arangoDB
    try {
      val colName = options.readOptions.collection.get
      val res = adb.execute(new Request.Builder[Void]()
        .db(options.readOptions.db)
        .method(Request.Method.GET)
        .path(s"/_api/collection/$colName/shards")
        .build(),
        classOf[RawBytes])
      val shardIds: Array[String] = adb.getSerde.deserialize(res.getBody.get, "/shards", classOf[Array[String]])
      client.shutdown()
      if (shardIds.isEmpty) {
        // Smart Edge collections return empty shardIds (BTS-1596)
        logWarning(s"Got empty shardIds for collection '$colName', reading will not be parallelized.")
        Array(null)
      } else {
        shardIds
      }
    } catch {
      case e: ArangoDBException =>
        client.shutdown()
        // single server < 3.8  returns Response: 500, Error: 4 - internal error
        // single server >= 3.8 returns Response: 501, Error: 9 - shards API is only available in a cluster
        if (INTERNAL_ERROR_CODE.equals(e.getErrorNum) || SHARDS_API_UNAVAILABLE_CODE.equals(e.getErrorNum)) {
          Array(null)
        } else {
          throw e
        }
    }
  }

  def acquireHostList(options: ArangoDBConf): Iterable[String] = {
    logDebug("acquiring host list")
    val client = ArangoClient(options)
    val adb = client.arangoDB
    val response = adb.execute(new Request.Builder[Void]
      .method(Request.Method.GET).path("/_api/cluster/endpoints").build(), classOf[RawBytes])
    val res = adb.getSerde
      .deserialize[Seq[Map[String, String]]](response.getBody.get, "/endpoints", classOf[Seq[Map[String, String]]])
      .map(it => it("endpoint").replaceFirst(".*://", ""))
    client.shutdown()
    res
  }

}
