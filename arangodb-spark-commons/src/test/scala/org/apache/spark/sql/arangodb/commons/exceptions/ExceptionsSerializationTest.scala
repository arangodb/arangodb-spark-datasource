package org.apache.spark.sql.arangodb.commons.exceptions

import com.arangodb.entity.ErrorEntity
import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.Test

import java.io.{ByteArrayOutputStream, ObjectOutputStream}

class ExceptionsSerializationTest {

  @Test
  def arangoDBMultiException(): Unit = {
    val mapper = new ObjectMapper()
    val errors = Stream.range(1, 10000).map(_ =>
      (
        mapper.readValue(
          """{
            |"errorMessage": "errorMessage",
            |"errorNum": 1234
            |}""".stripMargin, classOf[ErrorEntity]),
        "record"
      )
    )
    val e = new ArangoDBMultiException(errors.toArray)
    val objectOutputStream = new ObjectOutputStream(new ByteArrayOutputStream())
    objectOutputStream.writeObject(e)
    objectOutputStream.flush()
    objectOutputStream.close()
  }

}
