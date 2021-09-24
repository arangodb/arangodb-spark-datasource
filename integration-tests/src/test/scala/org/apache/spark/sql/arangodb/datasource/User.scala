package org.apache.spark.sql.arangodb.datasource

import java.sql.Date

case class User(
                 name: Name,
                 birthday: Date,
                 gender: String,
                 likes: List[String]
               )

case class Name(
                 first: String,
                 last: String
               )