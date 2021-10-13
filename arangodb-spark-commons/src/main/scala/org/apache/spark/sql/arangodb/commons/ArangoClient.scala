package org.apache.spark.sql.arangodb.commons

import com.arangodb.entity.ErrorEntity
import com.arangodb.internal.ArangoResponseField
import com.arangodb.internal.util.ArangoSerializationFactory.Serializer
import com.arangodb.mapping.ArangoJack
import com.arangodb.model.{AqlQueryOptions, CollectionCreateOptions}
import com.arangodb.velocypack.VPackSlice
import com.arangodb.velocystream.{Request, RequestType}
import com.arangodb.{ArangoCursor, ArangoDB}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.arangodb.commons.exceptions.ArangoDBMultiException
import org.apache.spark.sql.arangodb.commons.utils.PushDownCtx

import java.util
import scala.collection.JavaConverters.{asScalaIteratorConverter, mapAsJavaMapConverter}

// TODO: extend AutoCloseable
class ArangoClient(options: ArangoOptions) {

  private def aqlOptions(): AqlQueryOptions = {
    val opt = new AqlQueryOptions().stream(true)
    options.readOptions.fillBlockCache.foreach(opt.fillBlockCache(_))
    options.readOptions.cache.foreach(opt.cache(_))
    options.readOptions.batchSize.foreach(opt.batchSize(_))
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
        aqlOptions().shardIds(shardId),
        classOf[VPackSlice])
  }

  def readQuery(): ArangoCursor[VPackSlice] = arangoDB
    .db(options.readOptions.db)
    .query(
      options.readOptions.query.get,
      aqlOptions(),
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
      aqlOptions(),
      classOf[String])
    .asListRemaining()

  def readQuerySample(): util.List[String] = arangoDB
    .db(options.readOptions.db)
    .query(
      options.readOptions.query.get,
      aqlOptions(),
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
    options.writeOptions.waitForSync.foreach(request.putQueryParam("waitForSync", _))
    options.writeOptions.overwriteMode.foreach(request.putQueryParam("overwriteMode", _))
    options.writeOptions.keepNull.foreach(request.putQueryParam("keepNull", _))
    options.writeOptions.mergeObjects.foreach(request.putQueryParam("mergeObjects", _))

    request.setBody(data)
    val response = arangoDB.execute(request)

    // FIXME
    // in case there are no errors, response body is an empty object
    // In cluster 3.8.1 this is not true due to: https://arangodb.atlassian.net/browse/BTS-592
    if (response.getBody.isArray) {
      val errors = response.getBody.arrayIterator.asScala
        .filter(it => it.get(ArangoResponseField.ERROR).isTrue)
        .map(arangoDB.util().deserialize(_, classOf[ErrorEntity]).asInstanceOf[ErrorEntity])
        .toIterable
      if (errors.nonEmpty) {
        throw new ArangoDBMultiException(errors)
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