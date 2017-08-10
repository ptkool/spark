/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License")); you may not use this file except in compliance with
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

package org.apache.spark.sql.catalyst.expressions

import java.sql.{Date, Timestamp}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.types._

class TemporalExpressionSuite extends SparkFunSuite with ExpressionEvalHelper {

  test("CreatePeriod") {
    val d1 = Date.valueOf("2016-12-31")
    val d2 = Date.valueOf("2017-01-01")

    checkEvaluation(CreatePeriod(Literal(d1), Literal(d2)), Seq(d1, d2))
    checkEvaluation(CreatePeriod(Literal(d1)), Seq(d1, d2))
    checkEvaluation(CreatePeriod(Literal.create(null, DateType), Literal(d2)), null)

    val t1 = Timestamp.valueOf("2017-12-31 23:59:59.999999")
    val t2 = Timestamp.valueOf("2018-01-01 00:00:00.000000")

    checkEvaluation(CreatePeriod(Literal(t1), Literal(t2)), Seq(t1, t2))
    checkEvaluation(CreatePeriod(Literal(t1)), Seq(t1, t2))
    checkEvaluation(CreatePeriod(Literal.create(null, TimestampType), Literal(t2)), null)
  }

  test("PeriodBegin") {
    val d1 = Date.valueOf("2017-01-01")
    val d2 = Date.valueOf("2017-03-31")

    checkEvaluation(PeriodBegin(CreatePeriod(Literal(d1), Literal(d2))), d1)
    checkEvaluation(PeriodBegin(CreatePeriod(Literal.create(null, DateType), Literal(d2))),
      null)

    val t1 = Timestamp.valueOf("2017-01-01 10:30:02")
    val t2 = Timestamp.valueOf("2017-01-01 22:09:10")

    checkEvaluation(PeriodBegin(CreatePeriod(Literal(t1), Literal(t2))), t1)
  }

  test("PeriodEnd") {
    val d1 = Date.valueOf("2017-01-01")
    val d2 = Date.valueOf("2017-03-31")

    checkEvaluation(PeriodEnd(CreatePeriod(Literal(d1), Literal(d2))), d2)
    checkEvaluation(PeriodEnd(CreatePeriod(Literal(d1), Literal.create(null, DateType))), null)

    val t1 = Timestamp.valueOf("2017-01-01 10:30:02")
    val t2 = Timestamp.valueOf("2017-01-01 22:09:10")

    checkEvaluation(PeriodEnd(CreatePeriod(Literal(t1), Literal(t2))), t2)
  }

  test("PeriodLast") {
    val d1 = Date.valueOf("2017-01-01")
    val d2 = Date.valueOf("2017-03-31")
    val d3 = Date.valueOf("2017-03-30")

    checkEvaluation(PeriodLast(CreatePeriod(Literal(d1), Literal(d2))), d3)
    checkEvaluation(PeriodLast(CreatePeriod(Literal(d1), Literal.create(null, DateType))), null)

    val t1 = Timestamp.valueOf("2017-01-01 10:30:02")
    val t2 = Timestamp.valueOf("2017-01-01 22:09:10")
    val t3 = Timestamp.valueOf("2017-01-01 22:09:09.999999")

    checkEvaluation(PeriodLast(CreatePeriod(Literal(t1), Literal(t2))), t3)
  }

  test("PeriodEquals") {
    val d1 = Date.valueOf("2017-01-01")
    val d2 = Date.valueOf("2017-03-31")
    val d3 = Date.valueOf("2017-08-31")

    checkEvaluation(
      PeriodEquals(
        CreatePeriod(Literal(d1), Literal(d2)),
        CreatePeriod(Literal(d1), Literal(d2))
      ), true)

    checkEvaluation(
      PeriodEquals(
        CreatePeriod(Literal(d1), Literal(d2)),
        CreatePeriod(Literal(d1), Literal(d3))
      ), false)

    val t1 = Timestamp.valueOf("2017-01-01 10:30:22")
    val t2 = Timestamp.valueOf("2017-03-31 14:23:02")
    val t3 = Timestamp.valueOf("2017-08-31 22:00:00")

    checkEvaluation(
      PeriodEquals(
        CreatePeriod(Literal(t1), Literal(t2)),
        CreatePeriod(Literal(t1), Literal(t2))
      ), true)

    checkEvaluation(
      PeriodEquals(
        CreatePeriod(Literal(t1), Literal(t2)),
        CreatePeriod(Literal(t1), Literal(t3))
      ), false)
  }

  test("PeriodContains") {
    val d1 = Date.valueOf("2017-01-01")
    val d2 = Date.valueOf("2017-08-31")
    val d3 = Date.valueOf("2017-03-31")
    val d4 = Date.valueOf("2017-08-30")

    // PeriodContains(period, period)
    checkEvaluation(
      PeriodContains(
        CreatePeriod(Literal(d1), Literal(d2)),
        CreatePeriod(Literal(d1), Literal(d2))
      ), true)

    checkEvaluation(
      PeriodContains(
        CreatePeriod(Literal(d1), Literal(d2)),
        CreatePeriod(Literal(d1), Literal(d3))
      ), true)

    checkEvaluation(
      PeriodContains(
        CreatePeriod(Literal(d1), Literal(d2)),
        CreatePeriod(Literal(d3), Literal(d4))
      ), true)

    checkEvaluation(
      PeriodContains(
        CreatePeriod(Literal(d1), Literal(d3)),
        CreatePeriod(Literal(d4), Literal(d2))
      ), false)

    // PeriodContains(period, date)
    checkEvaluation(PeriodContains(CreatePeriod(Literal(d1), Literal(d2)), Literal(d1)), true)
    checkEvaluation(PeriodContains(CreatePeriod(Literal(d1), Literal(d2)), Literal(d3)), true)

    // PeriodContains(date, period)
    checkEvaluation(PeriodContains(Literal(d3), CreatePeriod(Literal(d1), Literal(d2))), false)
    checkEvaluation(PeriodContains(Literal(d4), CreatePeriod(Literal(d4), Literal(d2))), true)

    val t1 = Literal(Timestamp.valueOf("2017-01-01 10:30:22"))
    val t2 = Literal(Timestamp.valueOf("2017-08-31 14:23:02"))
    val t3 = Literal(Timestamp.valueOf("2017-03-31 22:00:00"))
    val t4 = Literal(Timestamp.valueOf("2017-06-30 00:00:00"))

    val p6 = CreatePeriod(t1, t2)
    val p7 = CreatePeriod(t1, t2)
    val p8 = CreatePeriod(t1, t3)
    val p9 = CreatePeriod(t3, t4)
    val p10 = CreatePeriod(t4, t2)

    checkEvaluation(PeriodContains(p6, p7), true)
    checkEvaluation(PeriodContains(p6, p8), true)
    checkEvaluation(PeriodContains(p6, p9), true)
    checkEvaluation(PeriodContains(p8, p10), false)
  }

  test("PeriodIntersect") {
    val d1 = Date.valueOf("2005-02-03")
    val d2 = Date.valueOf("2007-02-03")
    val d3 = Date.valueOf("2004-02-03")
    val d4 = Date.valueOf("2006-02-03")
    val d5 = Date.valueOf("2005-02-03")
    val d6 = Date.valueOf("2006-02-03")

    checkEvaluation(
      PeriodIntersect(
        CreatePeriod(Literal(d3), Literal(d4)),
        CreatePeriod(Literal(d1), Literal(d2))
      ), Seq(d5, d6))

    checkEvaluation(
      PeriodIntersect(
        CreatePeriod(Literal(d3), Literal(d1)),
        CreatePeriod(Literal(d5), Literal(d6))
      ), Seq(d5, d5))

    checkEvaluation(
      PeriodIntersect(
        CreatePeriod(Literal(d4), Literal(d2)),
        CreatePeriod(Literal(d3), Literal(d1))
      ), null)

    val t1 = Timestamp.valueOf("2005-02-03 10:10:10.100")
    val t2 = Timestamp.valueOf("2007-02-03 10:10:10.100")
    val t3 = Timestamp.valueOf("2004-02-03 10:10:10.000")
    val t4 = Timestamp.valueOf("2006-02-03 10:10:10.000")
    val t5 = Timestamp.valueOf("2005-02-03 10:10:10.100")
    val t6 = Timestamp.valueOf("2006-02-03 10:10:10.000")

    checkEvaluation(
      PeriodIntersect(
        CreatePeriod(Literal(t3), Literal(t4)),
        CreatePeriod(Literal(t1), Literal(t2))
      ), Array(t5, t6))
  }

  test("PeriodLDiff") {
    val d1 = Date.valueOf("2016-07-25")
    val d2 = Date.valueOf("2016-08-02")
    val d3 = Date.valueOf("2016-07-31")
    val d4 = Date.valueOf("2016-08-30")
    val d5 = Date.valueOf("2016-07-30")

    checkEvaluation(
      PeriodLDiff(
        CreatePeriod(Literal(d1), Literal(d2)),
        CreatePeriod(Literal(d3), Literal(d4))
      ),
      Seq(d1, d3)
    )

    checkEvaluation(
      PeriodLDiff(
        CreatePeriod(Literal(d1), Literal(d5)),
        CreatePeriod(Literal(d3), Literal(d4))
      ), null)

    checkEvaluation(
      PeriodLDiff(
        CreatePeriod(Literal(d3), Literal(d4)),
        CreatePeriod(Literal(d1), Literal(d2))
      ), null)

    checkEvaluation(
      PeriodLDiff(
        CreatePeriod(Literal.create(null, DateType), Literal(d5)),
        CreatePeriod(Literal(d3), Literal(d4))
      ), null)
  }

  test("PeriodRDiff") {
    val d1 = Date.valueOf("2017-02-03")
    val d2 = Date.valueOf("2018-02-03")
    val d3 = Date.valueOf("2017-04-02")
    val d4 = Date.valueOf("2018-01-03")
    val d5 = Date.valueOf("2017-02-01")

    checkEvaluation(
      PeriodRDiff(
        CreatePeriod(Literal(d1), Literal(d2)),
        CreatePeriod(Literal(d3), Literal(d4))
      ), Seq(d4, d2))

    checkEvaluation(
      PeriodRDiff(
        CreatePeriod(Literal(d1), Literal(d4)),
        CreatePeriod(Literal(d5), Literal(d1))
      ), Seq(d1, d4))

    checkEvaluation(
      PeriodRDiff(
        CreatePeriod(Literal(d2), Literal(d4)),
        CreatePeriod(Literal(d3), Literal(d4))
      ), null)

    checkEvaluation(
      PeriodRDiff(
        CreatePeriod(Literal.create(null, DateType), Literal(Date.valueOf("2016-07-30"))),
        CreatePeriod(Literal(Date.valueOf("2016-07-31")), Literal(Date.valueOf("2016-08-30")))
      ), null)
  }

  test("PeriodNormalize") {
    val t1 = Timestamp.valueOf("2004-02-03 10:10:10.000000")
    val t2 = Timestamp.valueOf("2006-02-03 10:10:10.000000")
    val t3 = Timestamp.valueOf("2005-02-03 10:10:10.100000")
    val t4 = Timestamp.valueOf("2007-02-03 10:10:10.100000")

    checkEvaluation(
      PeriodNormalize(
        CreatePeriod(Literal(t1), Literal(t2)),
        CreatePeriod(Literal(t3), Literal(t4))
      ), Seq(t1, t4))
  }

  test("PeriodOverlaps") {
    val d1 = Date.valueOf("2017-01-01")
    val d2 = Date.valueOf("2017-03-31")
    val d3 = Date.valueOf("2017-04-01")
    val d4 = Date.valueOf("2017-08-05")

    checkEvaluation(
      PeriodOverlaps(
        CreatePeriod(Literal(d1), Literal(d2)),
        CreatePeriod(Literal(d1), Literal(d2))
      ), true)

    checkEvaluation(
      PeriodOverlaps(
        CreatePeriod(Literal(d1), Literal(d2)),
        CreatePeriod(Literal(d2), Literal(d3))
      ), true)

    checkEvaluation(
      PeriodOverlaps(
        CreatePeriod(Literal(d1), Literal(d2)),
        CreatePeriod(Literal(d3), Literal(d4))
      ), false)

    checkEvaluation(
      PeriodOverlaps(
        CreatePeriod(Literal(d1), Literal(d2)),
        CreatePeriod(Literal(d1), Literal(d3))
      ), true)

    checkEvaluation(
      PeriodOverlaps(
        CreatePeriod(Literal(d2), Literal(d3)),
        CreatePeriod(Literal(d1), Literal(d2))
      ), true)

    checkEvaluation(
      PeriodOverlaps(
        CreatePeriod(Literal(d2), Literal(d3)),
        CreatePeriod(Literal(d1), Literal(d3))
      ), true)
  }

  test("PeriodMeets") {
    val d1 = Date.valueOf("2017-01-01")
    val d2 = Date.valueOf("2017-03-31")
    val d3 = Date.valueOf("2017-04-01")
    val d4 = Date.valueOf("2017-08-05")
    val d5 = Date.valueOf("2016-12-31")

    checkEvaluation(
      PeriodMeets(
        CreatePeriod(Literal(d1), Literal(d2)),
        CreatePeriod(Literal(d2), Literal(d3))
      ), true)

    checkEvaluation(
      PeriodMeets(
        CreatePeriod(Literal(d1), Literal(d2)),
        CreatePeriod(Literal(d3), Literal(d4))
      ), false)

    checkEvaluation(
      PeriodMeets(
        CreatePeriod(Literal(d1), Literal(d2)),
        Literal(d2)
      ), true)

    checkEvaluation(
      PeriodMeets(
        Literal(d1),
        CreatePeriod(Literal(d1), Literal(d2))
      ), false)

    checkEvaluation(
      PeriodMeets(
        Literal(d5),
        CreatePeriod(Literal(d1), Literal(d2))
      ), true)
  }

  test("PeriodPrecedes") {
    val d1 = Date.valueOf("2017-01-01")
    val d2 = Date.valueOf("2017-03-31")
    val d3 = Date.valueOf("2017-04-01")
    val d4 = Date.valueOf("2017-08-05")
    val d5 = Date.valueOf("2016-12-31")

    checkEvaluation(
      PeriodPrecedes(
        CreatePeriod(Literal(d1), Literal(d2)),
        CreatePeriod(Literal(d3), Literal(d4))
      ), true)

    checkEvaluation(
      PeriodPrecedes(
        CreatePeriod(Literal(d3), Literal(d4)),
        CreatePeriod(Literal(d2), Literal(d4))
      ), false)

    checkEvaluation(
      PeriodPrecedes(
        CreatePeriod(Literal(d1), Literal(d2)),
        Literal(d3)
      ), true)

    checkEvaluation(
      PeriodPrecedes(
        CreatePeriod(Literal(d3), Literal(d4)),
        Literal(d5)
      ), false)

    checkEvaluation(
      PeriodPrecedes(
        Literal(d5),
        CreatePeriod(Literal(d1), Literal(d2))
      ), true)

    checkEvaluation(
      PeriodPrecedes(
        CreatePeriod(Literal(d1), Literal(d2)),
        CreatePeriod(Literal(d1), Literal(d2))
      ), false)
  }

  test("PeriodSucceeds") {
    val d1 = Literal(Date.valueOf("2017-01-01"))
    val d2 = Literal(Date.valueOf("2017-03-31"))
    val d3 = Literal(Date.valueOf("2017-04-01"))
    val d4 = Literal(Date.valueOf("2017-08-05"))
    val d5 = Literal(Date.valueOf("2016-12-31"))

    checkEvaluation(
      PeriodSucceeds(
        CreatePeriod(Literal(d1), Literal(d2)),
        CreatePeriod(Literal(d3), Literal(d4))
      ), false)

    checkEvaluation(
      PeriodSucceeds(
        CreatePeriod(Literal(d3), Literal(d4)),
        CreatePeriod(Literal(d1), Literal(d2))
      ), true)

    checkEvaluation(
      PeriodSucceeds(
        CreatePeriod(Literal(d2), Literal(d4)),
        CreatePeriod(Literal(d1), Literal(d2))
      ), true)

    checkEvaluation(
      PeriodSucceeds(
        CreatePeriod(Literal(d1), Literal(d2)),
        Literal(d5)
      ), true)

    checkEvaluation(
      PeriodSucceeds(
        Literal(d2),
        CreatePeriod(Literal(d1), Literal(d2))
      ), true)

    checkEvaluation(
      PeriodSucceeds(
        CreatePeriod(Literal(d1), Literal(d2)),
        Literal(d1)
      ), false)
  }
}
