/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.catalyst.expressions.{CreatePeriod, Literal, PeriodIntersect}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{DateType, PeriodType, StructField, StructType}
import org.scalatest.exceptions.TestFailedException

class PeriodFunctionsSuite extends QueryTest with SharedSQLContext {

  import testImplicits._

  test("date period constructor") {
    val d1 = Date.valueOf("2018-01-01")
    val d2 = Date.valueOf("2018-01-02")

    val df1 = Seq((d1, d2)).toDF("d1", "d2")

    checkAnswer(df1.select(period($"d1", $"d2")), Row(Array(d1, d2)))
    checkAnswer(df1.select(period($"d1")), Row(Array(d1, d2)))

    checkAnswer(df1.selectExpr("period(d1, d2)"), Row(Array(d1, d2)))
    checkAnswer(df1.selectExpr("period(d1)"), Row(Array(d1, d2)))

    val schema = StructType(Seq(StructField("p", PeriodType(DateType))))
    val rowRDD = sparkContext.parallelize(Seq(Row(Array(d1, d2))))
    val df2 = spark.createDataFrame(rowRDD, schema)

    checkAnswer(df2.select("p"), Row(Array(d1, d2)))

    checkAnswer(sql("""SELECT PERIOD(DATE '2018-01-01', DATE '2018-01-02')"""), Row(Array(d1, d2)))
  }

  test("timestamp period constructor") {
    val t1 = Timestamp.valueOf("2018-01-01 23:59:59.999999")
    val t2 = Timestamp.valueOf("2018-01-02 00:00:00.000000")

    val df = Seq((t1, t2)).toDF("t1", "t2")

    checkAnswer(df.select(period($"t1", $"t2")), Row(Array(t1, t2)))
    checkAnswer(df.select(period($"t1")), Row(Array(t1, t2)))
  }

  test("function period_begin") {
    val d1 = Date.valueOf("2018-01-01")
    val d2 = Date.valueOf("2018-01-01")

    val df1 = Seq((d1, d2)).toDF("d1", "d2").withColumn("p", period($"d1", $"d2"))

    checkAnswer(df1.select(period_begin($"p")), Row(d1))

    val t1 = Timestamp.valueOf("2018-01-01 00:00:00")
    val t2 = Timestamp.valueOf("2018-01-01 00:00:00")

    val df2 = Seq((t1, t2)).toDF("t1", "t2").withColumn("p", period($"t1", $"t2"))

    checkAnswer(df2.select(period_begin($"p")), Row(t1))
  }

  test("function period_end") {
    val d1 = Date.valueOf("2018-01-01")
    val d2 = Date.valueOf("2018-01-01")

    val df1 = Seq((d1, d2)).toDF("d1", "d2").withColumn("p", period($"d1", $"d2"))

    checkAnswer(df1.select(period_end($"p")), Row(d2))

    val t1 = Timestamp.valueOf("2018-01-01 00:00:00")
    val t2 = Timestamp.valueOf("2018-01-01 00:00:00")

    val df2 = Seq((t1, t2)).toDF("t1", "t2").withColumn("p", period($"t1", $"t2"))

    checkAnswer(df2.select(period_end($"p")), Row(t2))
  }

  test("function period_last") {
    val d1 = Date.valueOf("2018-01-01")
    val d2 = Date.valueOf("2018-01-01")
    val d3 = Date.valueOf("2017-12-31")

    val df1 = Seq((d1, d2)).toDF("d1", "d2").withColumn("p", period($"d1", $"d2"))

    checkAnswer(df1.select(period_last($"p")), Row(d3))

    val t1 = Timestamp.valueOf("2018-01-01 00:00:00")
    val t2 = Timestamp.valueOf("2018-01-01 00:00:00")
    val t3 = Timestamp.valueOf("2017-12-31 23:59:59.999999")

    val df2 = Seq((t1, t2)).toDF("t1", "t2").withColumn("p", period($"t1", $"t2"))

    checkAnswer(df2.select(period_last($"p")), Row(t3))
  }

  test("function period_equals") {
    val d1 = Date.valueOf("2018-01-01")
    val d2 = Date.valueOf("2018-01-01")
    val d3 = Date.valueOf("2017-08-31")

    val df1 =
      Seq((d1, d2, d3))
        .toDF("d1", "d2", "d3")
        .withColumn("p1", period($"d1", $"d2"))
        .withColumn("p2", period($"d2", $"d3"))

    checkAnswer(
      df1.filter(period_equals($"p1", $"p1")),
      Row(d1, d2, d3, Array(d1, d2), Array(d1, d3))
    )

    checkAnswer(df1.filter(period_equals($"p1", $"p2")), Nil)

    val t1 = Timestamp.valueOf("2017-01-01 10:30:22")
    val t2 = Timestamp.valueOf("2017-03-31 14:23:02")
    val t3 = Timestamp.valueOf("2017-08-31 22:00:00")

    val df2 =
      Seq((t1, t2, t3))
        .toDF("t1", "t2", "t3")
        .withColumn("p1", period($"t1", $"t2"))
        .withColumn("p2", period($"t2", $"t3"))

    checkAnswer(
      df2.filter(period_equals($"p1", $"p1")),
      Row(t1, t2, t3, Array(t1, t2), Array(t2, t3))
    )

    checkAnswer(df2.filter(period_equals($"p1", $"p2")), Nil)
  }

  test("function period_contains") {
    val d1 = Date.valueOf("2018-07-25")
    val d2 = Date.valueOf("2018-09-30")
    val d3 = Date.valueOf("2018-07-31")
    val d4 = Date.valueOf("2018-08-30")
    val d5 = Date.valueOf("2018-10-01")

    val df1 =
      Seq((d1, d2, d3, d4, d5)).toDF("d1", "d2", "d3", "d4", "d5")
        .withColumn("p1", period($"d1", $"d2"))
        .withColumn("p2", period($"d3", $"d4"))
        .withColumn("p3", period($"d4", $"d5"))

    checkAnswer(df1.select(period_contains($"p1", $"p1")), Row(true))
    checkAnswer(df1.select(period_contains($"p1", $"p2")), Row(true))
    checkAnswer(df1.select(period_contains($"p1", $"p3")), Row(false))
    checkAnswer(df1.select(period_contains($"p1", $"d1")), Row(true))
    checkAnswer(df1.select(period_contains($"p1", $"d5")), Row(false))

    val t1 = Timestamp.valueOf("2018-07-25 10:30:22")
    val t2 = Timestamp.valueOf("2018-09-30 14:23:02")
    val t3 = Timestamp.valueOf("2018-07-31 22:00:00")
    val t4 = Timestamp.valueOf("2018-08-30 00:00:00")
    val t5 = Timestamp.valueOf("2018-10-01 00:00:00")

    val df2 =
      Seq((t1, t2, t3, t4, t5)).toDF("t1", "t2", "t3", "t4", "t5")
        .withColumn("p1", period($"t1", $"t2"))
        .withColumn("p2", period($"t3", $"t4"))
        .withColumn("p3", period($"t4", $"t5"))

    checkAnswer(df2.select(period_contains($"p1", $"p1")), Row(true))
    checkAnswer(df2.select(period_contains($"p1", $"p2")), Row(true))
    checkAnswer(df2.select(period_contains($"p1", $"p3")), Row(false))
    checkAnswer(df2.select(period_contains($"p1", $"t1")), Row(true))
    checkAnswer(df2.select(period_contains($"p1", $"t5")), Row(false))
  }

  test("function period_intersects") {
    val d1 = Date.valueOf("2005-02-03")
    val d2 = Date.valueOf("2007-02-03")
    val d3 = Date.valueOf("2004-02-03")
    val d4 = Date.valueOf("2006-02-03")
    val d5 = Date.valueOf("2005-02-03")
    val d6 = Date.valueOf("2006-02-03")

    val df1 = Seq((d1, d2, d3, d4, d5, d6))
      .toDF("d1", "d2", "d3", "d4", "d5", "d6")
      .withColumn("p1", period($"d1", $"d2"))
      .withColumn("p2", period($"d3", $"d4"))
      .withColumn("p3", period($"d3", $"d1"))
      .withColumn("p4", period($"d5", $"d6"))
      .withColumn("p5", period($"d4", $"d2"))
      .withColumn("p6", period($"d3", $"d1"))

    checkAnswer(df1.select(period_intersect($"p2", $"p1")), Row(Seq(d5, d6)))
    checkAnswer(df1.select(period_intersect($"p3", $"p4")), Row(Seq(d5, d5)))
    checkAnswer(df1.select(period_intersect($"p5", $"p6")), Row(null))

    val t1 = Timestamp.valueOf("2005-02-03 10:10:10.100")
    val t2 = Timestamp.valueOf("2007-02-03 10:10:10.100")
    val t3 = Timestamp.valueOf("2004-02-03 10:10:10.000")
    val t4 = Timestamp.valueOf("2006-02-03 10:10:10.000")
    val t5 = Timestamp.valueOf("2005-02-03 10:10:10.100")
    val t6 = Timestamp.valueOf("2006-02-03 10:10:10.000")

    val df2 = Seq((t1, t2, t3, t4, t5, t6))
      .toDF("t1", "t2", "t3", "t4", "t5", "t6")
      .withColumn("p1", period($"t1", $"t2"))
      .withColumn("p2", period($"t3", $"t4"))
      .withColumn("p3", period($"t3", $"t1"))
      .withColumn("p4", period($"t5", $"t6"))
      .withColumn("p5", period($"t4", $"t2"))
      .withColumn("p6", period($"t3", $"t1"))

    checkAnswer(df2.select(period_intersect($"p2", $"p1")), Row(Seq(t5, t6)))
    checkAnswer(df2.select(period_intersect($"p3", $"p4")), Row(Seq(t5, t5)))
    checkAnswer(df2.select(period_intersect($"p5", $"p6")), Row(null))
  }

  test("period function error handling") {
    val d1 = Date.valueOf("2005-02-03")
    val d2 = Date.valueOf("2007-02-03")
    val d3 = Date.valueOf("2004-02-03")
    val d4 = Date.valueOf("2006-02-03")

    val t1 = Timestamp.valueOf("2005-02-03 10:10:10.100")
    val t2 = Timestamp.valueOf("2007-02-03 10:10:10.100")
    val t3 = Timestamp.valueOf("2004-02-03 10:10:10.000")
    val t4 = Timestamp.valueOf("2006-02-03 10:10:10.000")

    val df = Seq((d1, d2, d3, d4, t1, t2, t3, t4))
      .toDF("d1", "d2", "d3", "d4", "t1", "t2", "t3", "t4")
      .withColumn("p1", period($"d1", $"d2"))
      .withColumn("p2", period($"d3", $"d4"))
      .withColumn("p3", period($"t1", $"t2"))
      .withColumn("p4", period($"t3", $"t4"))

    val e1 = intercept[AnalysisException] {
      df.select(period($"d1", $"t1")).collect()
    }

    val e2 = intercept[AnalysisException] {
      df.select(period_equals($"p1", $"p3")).collect()
    }

    val e3 = intercept[AnalysisException] {
      df.select(period_contains($"d1", $"p1")).collect()
    }

    val e4 = intercept[AnalysisException] {
      df.select(period_contains($"p1", $"p3")).collect()
    }

    val e5 = intercept[AnalysisException] {
      df.select(period_intersect($"t1", $"p1")).collect()
    }

    val e6 = intercept[AnalysisException] {
      df.select(period_intersect($"p1", $"p3")).collect()
    }

    // assert(e1.getMessage.contains())
    assert(e2.getMessage.contains("input to function equals should all be the same type"))
    assert(e3.getMessage.contains("First argument must be of type period(date), not date"))
    assert(e4.getMessage.contains("Element types of periods period(date) and period(timestamp)" +
      " do not match"))
    assert(e5.getMessage.contains("First argument must be of type period(date), not timestamp"))
    assert(e6.getMessage.contains("Element types of periods period(date) and period(timestamp)" +
      " do not match"))

  }
}