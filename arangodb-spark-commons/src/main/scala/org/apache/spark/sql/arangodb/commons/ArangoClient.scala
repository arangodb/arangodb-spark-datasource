package org.apache.spark.sql.arangodb.commons

import com.arangodb.entity.ErrorEntity
import com.arangodb.internal.util.ArangoSerializationFactory.Serializer
import com.arangodb.internal.{ArangoRequestParam, ArangoResponseField}
import com.arangodb.mapping.ArangoJack
import com.arangodb.model.{AqlQueryOptions, CollectionCreateOptions}
import com.arangodb.velocypack.VPackSlice
import com.arangodb.velocystream.{Request, RequestType}
import com.arangodb.{ArangoCursor, ArangoDB, ArangoDBException}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.internal.Logging
import org.apache.spark.sql.arangodb.commons.exceptions.ArangoDBMultiException
import org.apache.spark.sql.arangodb.commons.filter.PushableFilter
import org.apache.spark.sql.types.StructType

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

  lazy val arangoDB: ArangoDB = options.driverOptions
    .builder()
    .serializer(new ArangoJack() {
      //noinspection ConvertExpressionToSAM
      configure(new ArangoJack.ConfigureFunction {
        override def configure(mapper: ObjectMapper): Unit = mapper.registerModule(DefaultScalaModule)
      })
    })
    .build()

  def shutdown(): Unit = arangoDB.shutdown()

  def readCollectionPartition(shardId: String, filters: Array[PushableFilter], schema: StructType): ArangoCursor[VPackSlice] = {
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
      .query(query, params.asJava, opts, classOf[VPackSlice])
  }

  def readQuery(): ArangoCursor[VPackSlice] = {
    val query = options.readOptions.query.get
    logDebug(s"Executing AQL query: \n\t$query")
    arangoDB
      .db(options.readOptions.db)
      .query(
        query,
        aqlOptions(),
        classOf[VPackSlice])
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
      .query(query, params.asJava, opts, classOf[String])
      .asListRemaining()
      .asScala
      .toSeq
  }

  def readQuerySample(): Seq[String] = {
    val query = options.readOptions.query.get
    logDebug(s"Executing AQL query: \n\t$query")
    val cursor = arangoDB
      .db(options.readOptions.db)
      .query(
        query,
        aqlOptions(),
        classOf[String])

    import scala.collection.JavaConverters.asScalaIteratorConverter
    cursor.asScala.take(options.readOptions.sampleSize).toSeq
  }

  def collectionExists(): Boolean = arangoDB
    .db(options.writeOptions.db)
    .collection(options.writeOptions.collection)
    .exists()

  def createCollection(): Unit = {
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
    try {
      arangoDB
        .db(options.writeOptions.db)
        .collection(options.writeOptions.collection)
        .truncate()
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

  def drop(): Unit = arangoDB
    .db(options.writeOptions.db)
    .collection(options.writeOptions.collection)
    .drop()

  def saveDocuments(data: VPackSlice): Unit = {
    val request = new Request(
      options.writeOptions.db,
      RequestType.POST,
      s"/_api/document/${options.writeOptions.collection}")

    request.putQueryParam("silent", true)
    request.putQueryParam("waitForSync", options.writeOptions.waitForSync)
    request.putQueryParam("overwriteMode", options.writeOptions.overwriteMode.getValue)
    request.putQueryParam("keepNull", options.writeOptions.keepNull)
    request.putQueryParam("mergeObjects", options.writeOptions.mergeObjects)

    request.setBody(data)
    val response = arangoDB.execute(request)

    import scala.collection.JavaConverters.asScalaIteratorConverter
    // FIXME
    // in case there are no errors, response body is an empty object
    // In cluster 3.8.1 this is not true due to: https://arangodb.atlassian.net/browse/BTS-592
    if (response.getBody.isArray) {
      val errors = response.getBody.arrayIterator.asScala
        .filter(it => it.get(ArangoResponseField.ERROR).isTrue)
        .map(arangoDB.util().deserialize[ErrorEntity](_, classOf[ErrorEntity]))
        .toArray
      if (errors.nonEmpty) {
        throw new ArangoDBMultiException(errors)
      }
    }
  }
}


@SuppressWarnings(Array("OptionGet"))
object ArangoClient {
  private val INTERNAL_ERROR_CODE = 4
  private val SHARDS_API_UNAVAILABLE_CODE = 9

  def apply(options: ArangoDBConf): ArangoClient = new ArangoClient(options)

  def getCollectionShardIds(options: ArangoDBConf): Array[String] = {
    try {
      val client = ArangoClient(options).arangoDB
      val res = client.execute(new Request(
        options.readOptions.db,
        RequestType.GET,
        s"/_api/collection/${options.readOptions.collection.get}/shards"))
      val shardIds: Array[String] = client.util().deserialize(res.getBody.get("shards"), classOf[Array[String]])
      client.shutdown()
      shardIds
    } catch {
      case e: ArangoDBException =>
        // single server < 3.8  returns Response: 500, Error: 4 - internal error
        // single server >= 3.8 returns Response: 501, Error: 9 - shards API is only available in a cluster
        if (INTERNAL_ERROR_CODE.equals(e.getErrorNum) || SHARDS_API_UNAVAILABLE_CODE.equals(e.getErrorNum)) {
          Array("")
        } else {
          throw e
        }
    }
  }

  def acquireHostList(options: ArangoDBConf): Iterable[String] = {
    val client = ArangoClient(options).arangoDB
    val response = client.execute(new Request(ArangoRequestParam.SYSTEM, RequestType.GET, "/_api/cluster/endpoints"))
    val field = response.getBody.get("endpoints")
    val res = client.util(Serializer.CUSTOM)
      .deserialize[Seq[Map[String, String]]](field, classOf[Seq[Map[String, String]]])
      .map(it => it("endpoint").replaceFirst(".*://", ""))
    client.shutdown()
    res
  }

}
