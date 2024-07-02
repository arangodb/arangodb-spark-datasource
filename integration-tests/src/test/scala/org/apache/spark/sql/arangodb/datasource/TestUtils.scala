package org.apache.spark.sql.arangodb.datasource

object TestUtils {
  def isAtLeastVersion(version: String, otherMajor: Int, otherMinor: Int, otherPatch: Int): Boolean = compareVersion(version, otherMajor, otherMinor, otherPatch) >= 0

  def isLessThanVersion(version: String, otherMajor: Int, otherMinor: Int, otherPatch: Int): Boolean = compareVersion(version, otherMajor, otherMinor, otherPatch) < 0

  private def compareVersion(version: String, otherMajor: Int, otherMinor: Int, otherPatch: Int): Int = {
    val parts = version.split("-")(0).split("\\.")
    val major = parts(0).toInt
    val minor = parts(1).toInt
    val patch = parts(2).toInt
    val majorComparison = Integer.compare(major, otherMajor)
    if (majorComparison != 0) {
      return majorComparison
    }
    val minorComparison = Integer.compare(minor, otherMinor)
    if (minorComparison != 0) {
      return minorComparison
    }
    Integer.compare(patch, otherPatch)
  }
}
