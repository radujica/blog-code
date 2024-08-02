package scalaudfs

import org.apache.spark.sql.api.java.UDF1

class AddOne extends UDF1[Int, Int] {

  override def call(x: Int): Int = {
    x + 1
  }
}
