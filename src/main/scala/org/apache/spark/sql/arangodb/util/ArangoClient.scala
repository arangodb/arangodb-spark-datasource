package org.apache.spark.sql.arangodb.util

import com.arangodb.internal.util.ArangoSerializationFactory.Serializer
import com.arangodb.mapping.ArangoJack
import com.arangodb.model.AqlQueryOptions
import com.arangodb.velocypack.VPackSlice
import com.arangodb.velocystream.{Request, RequestType}
import com.arangodb.{ArangoCursor, ArangoDB}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.arangodb.datasource.ArangoOptions
import org.apache.spark.sql.types.{StructField, StructType}

import java.util
import scala.collection.JavaConverters.mapAsJavaMapConverter

class ArangoClient(options: ArangoOptions) {

  lazy val arangoDB: ArangoDB = options.driverOptions
    .builder()
    .serializer(new ArangoJack() {
      configure(f => f.registerModule(DefaultScalaModule))
    })
    .build()

  def shutdown(): Unit = arangoDB.shutdown()

  def readCollectionPartition(shardId: String, requiredSchema: StructType): ArangoCursor[VPackSlice] = arangoDB
    .db(options.readOptions.db)
    .query(
      s"FOR d IN @@col RETURN ${ArangoClient.generateColumnsFilter(requiredSchema, "d")}",
      Map[String, AnyRef]("@col" -> options.readOptions.collection.get).asJava,
      new AqlQueryOptions()
        //        .ttl()
        .batchSize(options.readOptions.batchSize)
        .stream(true)
        .shardIds(shardId),
      classOf[VPackSlice])

  def readQuery(): ArangoCursor[VPackSlice] = arangoDB
    .db(options.readOptions.db)
    .query(
      options.readOptions.query.get,
      new AqlQueryOptions()
        //        .ttl()
        .batchSize(options.readOptions.batchSize)
        .stream(true),
      classOf[VPackSlice])

  def readCollectionSample(): util.List[String] = arangoDB
    .db(options.readOptions.db)
    .query(
      "FOR d IN @@col LIMIT @size RETURN d",
      Map(
        "@col" -> options.readOptions.collection.get,
        "size" -> options.readOptions.sampleSize,
      )
        .asInstanceOf[Map[String, AnyRef]]
        .asJava,
      new AqlQueryOptions()
        //        .ttl()
        .batchSize(options.readOptions.batchSize)
        .stream(true),
      classOf[String])
    .asListRemaining()

  def readQuerySample(): util.List[String] = arangoDB
    .db(options.readOptions.db)
    .query(
      options.readOptions.query.get,
      new AqlQueryOptions()
        //        .ttl()
        .batchSize(options.readOptions.batchSize)
        .stream(true),
      classOf[String])
    .asListRemaining()

  def saveDocuments(data: VPackSlice): Unit = {
    val request = new Request(
      options.writeOptions.db,
      RequestType.POST,
      s"/_api/document/${options.writeOptions.collection}")
    request.setBody(data)
    val response = arangoDB.execute(request)
    println(response.getResponseCode)
  }
}


object ArangoClient {

  def apply(options: ArangoOptions): ArangoClient = new ArangoClient(options)

  def getCollectionShardIds(options: ArangoOptions): Array[String] = {
    val client = ArangoClient(options).arangoDB
    val res = client.execute(new Request(
      options.readOptions.db,
      RequestType.GET,
      s"/_api/collection/${options.readOptions.collection.get}/shards"))
    val shardIds: Array[String] = client.util(Serializer.CUSTOM).deserialize(res.getBody.get("shards"), classOf[Array[String]])
    client.shutdown()
    shardIds
  }

  private[util] def generateColumnsFilter(schema: StructType, documentVariable: String): String =
    doGenerateColumnsFilter(schema, s"`$documentVariable`.")

  private def doGenerateColumnsFilter(schema: StructType, ctx: String): String = "{" +
    schema.fields.map(generateFieldFilter(_, ctx)).mkString(",") + "}"

  private def generateFieldFilter(field: StructField, ctx: String): String = {
    val fieldName = s"`${field.name}`"
    val value = s"$ctx$fieldName"
    s"$fieldName:" + (field.dataType match {
      case s: StructType => doGenerateColumnsFilter(s, s"$value.")
      case _ => value
    })
  }

}