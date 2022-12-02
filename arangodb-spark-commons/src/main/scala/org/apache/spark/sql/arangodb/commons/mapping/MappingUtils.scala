package org.apache.spark.sql.arangodb.commons.mapping

import com.arangodb.jackson.dataformat.velocypack.VPackMapper
import com.fasterxml.jackson.databind.ObjectMapper

object MappingUtils {

  private val jsonMapper = new ObjectMapper()
  private val vpackMapper = new VPackMapper()

  def vpackToJson(in: Array[Byte]): String = jsonMapper.writeValueAsString(vpackMapper.readTree(in))

  def jsonToVPack(in: String): Array[Byte] = vpackMapper.writeValueAsBytes(jsonMapper.readTree(in))

}
