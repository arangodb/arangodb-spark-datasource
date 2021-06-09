package org.apache.spark.sql.arangodb.commons.utils

import org.apache.spark.sql.arangodb.commons.filter.PushableFilter
import org.apache.spark.sql.types.StructType

class PushDownCtx(
                   // columns projection to return
                   val requiredSchema: StructType,

                   // filters to push down
                   val filters: Array[PushableFilter]
                 )
  extends Serializable