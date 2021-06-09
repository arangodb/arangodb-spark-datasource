package org.apache.spark.sql.arangodb.commons.filter

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, MethodSource}

import java.util.stream

class PackageTest {
  @ParameterizedTest
  @MethodSource(Array("provideSplitAttributeName"))
  def splitAttributeName(attribute: String, expected: Array[String]): Unit = {
    assertThat(splitAttributeNameParts(attribute)).isEqualTo(expected)
  }
}

object PackageTest {
  def provideSplitAttributeName: stream.Stream[Arguments] =
    stream.Stream.of(
      Arguments.of("a.b.c.d.e.f", Array("a", "b", "c", "d", "e", "f")),
      Arguments.of("a.`b`.c.d.e.f", Array("a", "b", "c", "d", "e", "f")),
      Arguments.of("a.`b.c`.d.e.f", Array("a", "b.c", "d", "e", "f")),
      Arguments.of("a.b.`c.d`.e.f", Array("a", "b", "c.d", "e", "f")),
      Arguments.of("a.b.`.c.d.`.e.f", Array("a", "b", ".c.d.", "e", "f")),
      Arguments.of("a.b.`.`.e.f", Array("a", "b", ".", "e", "f"))
    )
}