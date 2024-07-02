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

import com.arangodb.entity

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

sealed trait ContentType {
  val name: String
}

object ContentType {
  case object JSON extends ContentType {
    override val name: String = "json"
  }

  case object VPACK extends ContentType {
    override val name: String = "vpack"
  }

  def apply(value: String): ContentType = value match {
    case JSON.name => JSON
    case VPACK.name => VPACK
    case _ => throw new IllegalArgumentException(s"${ArangoDBConf.CONTENT_TYPE}: $value")
  }
}

sealed trait Protocol {
  val name: String
}

object Protocol {
  case object VST extends Protocol {
    override val name: String = "vst"
  }

  case object HTTP extends Protocol {
    override val name: String = "http"
  }

  case object HTTP2 extends Protocol {
    override val name: String = "http2"
  }

  def apply(value: String): Protocol = value match {
    case VST.name => VST
    case HTTP.name => HTTP
    case HTTP2.name => HTTP2
    case _ => throw new IllegalArgumentException(s"${ArangoDBConf.PROTOCOL}: $value")
  }
}

sealed trait CollectionType {
  val name: String

  def get(): entity.CollectionType
}

object CollectionType {
  case object DOCUMENT extends CollectionType {
    override val name: String = "document"

    override def get(): entity.CollectionType = entity.CollectionType.DOCUMENT
  }

  case object EDGE extends CollectionType {
    override val name: String = "edge"

    override def get(): entity.CollectionType = entity.CollectionType.EDGES
  }

  def apply(value: String): CollectionType = value match {
    case DOCUMENT.name => DOCUMENT
    case EDGE.name => EDGE
    case _ => throw new IllegalArgumentException(s"${ArangoDBConf.COLLECTION_TYPE}: $value")
  }
}
