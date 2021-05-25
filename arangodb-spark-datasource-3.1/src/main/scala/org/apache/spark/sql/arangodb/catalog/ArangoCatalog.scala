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
package org.apache.spark.sql.arangodb.catalog

import com.arangodb.ArangoDB
import com.arangodb.mapping.ArangoJack
import com.arangodb.model.CollectionsReadOptions
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.arangodb.datasource.{ArangoOptions, ArangoTable}
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import scala.collection.JavaConverters._


// TODO
class ArangoCatalog
  extends CatalogPlugin
    with TableCatalog
    with SupportsNamespaces {

  private val arangoDB = new ArangoDB.Builder()
    .serializer(new ArangoJack() {
      configure(f => f.registerModule(DefaultScalaModule))
    })
    .build()

  override def name(): String = ArangoCatalog.NAME

  override def defaultNamespace(): Array[String] = {
    Array("_system")
  }

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    println(s">>> initialize($name, ${options.asScala})")
  }

  override def listNamespaces(): Array[Array[String]] = {
    println(">>> listNamespaces()")
    arangoDB.getDatabases.asScala
      .map(i => Array(i))
      .toArray
  }

  override def listTables(namespace: Array[String]): Array[Identifier] = {
    println(s">>> listTables(${namespace.toSeq})")
    val db = arangoDB.db(namespace.head)
    if (namespace.length != 1 || !db.exists())
      throw new NoSuchNamespaceException(namespace)

    db.getCollections(new CollectionsReadOptions().excludeSystem(true)).asScala
      .map(collection => Identifier.of(namespace, collection.getName))
      .toArray
  }

  override def toString = s"${this.getClass.getCanonicalName}(${name()})"

  override def loadTable(ident: Identifier): Table = {
    println(s">>> loadTable($ident)")
    new ArangoTable(null, ArangoOptions(Map(
      "database" -> "sparkConnectorTest",
      "user" -> "root",
      "password" -> "test",
      "endpoints" -> "172.28.3.1:8529,172.28.3.2:8529,172.28.3.3:8529",
      "table" -> "users"
    )))
  }

  override def createTable(ident: Identifier, schema: StructType, partitions: Array[Transform], properties: util.Map[String, String]): Table = ???

  override def alterTable(ident: Identifier, changes: TableChange*): Table = ???

  override def dropTable(ident: Identifier): Boolean = ???

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = ???

  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = ???

  override def createNamespace(namespace: Array[String], metadata: util.Map[String, String]): Unit = ???

  override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit = ???

  override def dropNamespace(namespace: Array[String]): Boolean = ???

  override def loadNamespaceMetadata(namespace: Array[String]): util.Map[String, String] = ???

}

object ArangoCatalog {
  val NAME = "arango"
}
