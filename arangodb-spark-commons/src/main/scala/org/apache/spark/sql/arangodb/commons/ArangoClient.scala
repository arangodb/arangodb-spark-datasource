package org.apache.spark.sql.arangodb.commons

import com.arangodb.internal.ArangoResponseField
import com.arangodb.internal.util.ArangoSerializationFactory.Serializer
import com.arangodb.mapping.ArangoJack
import com.arangodb.model.{AqlQueryOptions, CollectionCreateOptions}
import com.arangodb.velocypack.{VPackParser, VPackSlice}
import com.arangodb.velocystream.{Request, RequestType}
import com.arangodb.{ArangoCursor, ArangoDB}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.arangodb.commons.exceptions.ArangoDBServerException
import org.apache.spark.sql.arangodb.commons.utils.PushDownCtx

import java.util
import scala.collection.JavaConverters.{asScalaIteratorConverter, mapAsJavaMapConverter}

// TODO: extend AutoCloseable
class ArangoClient(options: ArangoOptions) {

  private val aqlOptions = new AqlQueryOptions()
    .cache(options.readOptions.cache)
    .fillBlockCache(options.readOptions.fillBlockCache)
    .batchSize(options.readOptions.batchSize)
    .stream(true)

  private val errorParser = new VPackParser.Builder().build()

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

  def readCollectionPartition(shardId: String, ctx: PushDownCtx): ArangoCursor[VPackSlice] = {
    val query =
      s"""
         |FOR d IN @@col
         |${PushdownUtils.generateFilterClause(ctx.filters)}
         |RETURN ${PushdownUtils.generateColumnsFilter(ctx.requiredSchema, "d")}"""
        .stripMargin
        .replaceAll("\n", " ")

    arangoDB
      .db(options.readOptions.db)
      .query(query,
        Map[String, AnyRef]("@col" -> options.readOptions.collection.get).asJava,
        new AqlQueryOptions()
          //        .ttl()
          .batchSize(options.readOptions.batchSize)
          .stream(true)
          .shardIds(shardId),
        classOf[VPackSlice])
  }

  def readQuery(): ArangoCursor[VPackSlice] = arangoDB
    .db(options.readOptions.db)
    .query(
      options.readOptions.query.get,
      aqlOptions,
      classOf[VPackSlice])

  def readCollectionSample(): util.List[String] = arangoDB
    .db(options.readOptions.db)
    .query(
      "FOR d IN @@col LIMIT @size RETURN d",
      Map(
        "@col" -> options.readOptions.collection.get,
        "size" -> options.readOptions.sampleSize
      )
        .asInstanceOf[Map[String, AnyRef]]
        .asJava,
      aqlOptions,
      classOf[String])
    .asListRemaining()

  def readQuerySample(): util.List[String] = arangoDB
    .db(options.readOptions.db)
    .query(
      options.readOptions.query.get,
      aqlOptions,
      classOf[String])
    .asListRemaining()

  def collectionExists(): Boolean = arangoDB
    .db(options.writeOptions.db)
    .collection(options.writeOptions.collection)
    .exists()

  def createCollection(): Unit = arangoDB
    .db(options.writeOptions.db)
    .collection(options.writeOptions.collection)
    .create(new CollectionCreateOptions()
      // TODO:
      //      .`type`()
      //      .numberOfShards()
      //      .replicationFactor()
      //      .minReplicationFactor()
    )

  def truncate(): Unit = arangoDB
    .db(options.writeOptions.db)
    .collection(options.writeOptions.collection)
    .truncate()

  def saveDocuments(data: VPackSlice): Unit = {
    val request = new Request(
      options.writeOptions.db,
      RequestType.POST,
      s"/_api/document/${options.writeOptions.collection}")

    request.putQueryParam("waitForSync", options.writeOptions.waitForSync)
    request.putQueryParam("silent", true)
    request.putQueryParam("overwriteMode", options.writeOptions.overwriteMode)
    request.putQueryParam("keepNull", options.writeOptions.keepNull)
    request.putQueryParam("mergeObjects", options.writeOptions.mergeObjects)

    request.setBody(data)
    val response = arangoDB.execute(request)
    println(response.getResponseCode)

    // in case there are no errors, response body is an empty object
    if (response.getBody.isArray) {
      val errors = response.getBody.arrayIterator.asScala
        .filter(it => it.get(ArangoResponseField.ERROR).isTrue)
        .map(it => errorParser.toJson(it, true))
      if (errors.nonEmpty) {
        throw new ArangoDBServerException(errors.mkString("[\n\t", ",\n\t", "\n]"))
      }
    }
  }
}


object ArangoClient {

  def apply(options: ArangoOptions): ArangoClient = new ArangoClient(options)

  def getCollectionShardIds(options: ArangoOptions): Array[String] = options.readOptions.arangoTopology match {
    case ArangoTopology.SINGLE => Array("")
    case ArangoTopology.CLUSTER =>
      val client = ArangoClient(options).arangoDB
      val res = client.execute(new Request(
        options.readOptions.db,
        RequestType.GET,
        s"/_api/collection/${options.readOptions.collection.get}/shards"))
      val shardIds: Array[String] = client.util(Serializer.CUSTOM).deserialize(res.getBody.get("shards"), classOf[Array[String]])
      client.shutdown()
      shardIds
  }

}