package org.apache.spark.sql.arangodb.datasource

import com.arangodb.entity.EdgeDefinition
import com.arangodb.model.GraphCreateOptions
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.arangodb.commons.ArangoDBConf
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource

import java.util
import scala.collection.JavaConverters.asJavaIterableConverter
import scala.collection.immutable
import scala.jdk.CollectionConverters.asJavaCollectionConverter

class ReadSmartEdgeCollectionTest extends BaseSparkTest {

  @ParameterizedTest
  @MethodSource(Array("provideProtocolAndContentType"))
  def readSmartEdgeCollection(protocol: String, contentType: String): Unit = {
    val df: DataFrame = spark.read
      .format(BaseSparkTest.arangoDatasource)
      .options(options + (
        ArangoDBConf.COLLECTION -> ReadSmartEdgeCollectionTest.name,
        ArangoDBConf.PROTOCOL -> protocol,
        ArangoDBConf.CONTENT_TYPE -> contentType
      ))
      .load()


    import spark.implicits._
    val read = df
      .as[Edge]
      .collect()

    assertThat(read.map(_.name)).containsAll(ReadSmartEdgeCollectionTest.data.map(d => d("name")).asJava)
  }

}

object ReadSmartEdgeCollectionTest {
  val name = "smartEdgeCol"
  val from = s"from-$name"
  val to = s"from-$name"

  val data: immutable.Seq[Map[String, String]] = (1 to 10)
    .map(x => Map(
      "name" -> s"name-$x",
      "_from" -> s"$from/a:$x",
      "_to" -> s"$to/b:$x"
    ))

  @BeforeAll
  def init(): Unit = {
    assumeTrue(BaseSparkTest.isCluster && BaseSparkTest.isEnterprise)

    if (BaseSparkTest.db.graph(name).exists()) {
      BaseSparkTest.db.graph(name).drop(true)
    }

    val ed = new EdgeDefinition()
      .collection(name)
      .from(from)
      .to(to)
    val opts = new GraphCreateOptions()
      .numberOfShards(2)
      .isSmart(true)
      .smartGraphAttribute("name")
    BaseSparkTest.db.createGraph(name, List(ed).asJavaCollection, opts)
    BaseSparkTest.db.collection(name).insertDocuments(data.asJava.asInstanceOf[util.Collection[Any]])
  }
}

case class Edge(
                 name: String,
                 _from: String,
                 _to: String
               )
