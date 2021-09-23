package org.apache.spark.sql.arangodb.commons

import com.arangodb.internal.util.ArangoSerializationFactory.Serializer
import com.arangodb.mapping.ArangoJack
import com.arangodb.model.AqlQueryOptions
import com.arangodb.velocypack.VPackSlice
import com.arangodb.velocystream.{Request, RequestType}
import com.arangodb.{ArangoCursor, ArangoDB}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.arangodb.commons.utils.PushDownCtx

import java.util
import scala.collection.JavaConverters.mapAsJavaMapConverter

class ArangoClient(options: ArangoOptions) {

  private val aqlOptions = new AqlQueryOptions()
    .cache(options.readOptions.cache)
    .fillBlockCache(options.readOptions.fillBlockCache)
    .batchSize(options.readOptions.batchSize)
    .stream(true)

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

  def saveDocuments(data: VPackSlice): Unit = {
    val request = new Request(
      options.writeOptions.db,
      RequestType.POST,
      s"/_api/document/${options.writeOptions.collection}")

    request.putQueryParam("waitForSync", options.writeOptions.waitForSync)
    request.putQueryParam("silent", true)

    // TODO:
    //    request.putQueryParam("overwriteMode", ...);

    request.setBody(data)
    val response = arangoDB.execute(request)
    println(response.getResponseCode)
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