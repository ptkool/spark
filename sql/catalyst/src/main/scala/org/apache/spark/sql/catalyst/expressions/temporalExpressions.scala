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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.util.DateTimeUtils.{SQLDate, SQLTimestamp}
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData, TypeUtils}
import org.apache.spark.sql.types
import org.apache.spark.sql.types.{TimestampType, _}

/**
 * Returns the beginning bound of a period.
 */
@ExpressionDescription(
  usage = "FUNC_(period) - Returns the beginning bound of a `period` expression.",
  extended = """
    Examples:
      > SELECT _FUNC_(PERIOD('2016-07-25', '2016-07-31'));
       2016-07-25
  """)
case class PeriodBegin(period: Expression)
  extends UnaryExpression with ExpectsInputTypes {

  override def child: Expression = period

  override def inputTypes: Seq[AbstractDataType] = Seq(PeriodType)

  override def dataType: DataType = child.dataType.asInstanceOf[PeriodType].elementType

  override def nullSafeEval(input: Any): Any = {
    input.asInstanceOf[ArrayData].get(0, dataType)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, p => {
      s"""${ev.value} = ${ctx.getValue(p, dataType, String.valueOf(0))};"""
    })
  }

  override def prettyName: String = "begin"
}

/**
 * Returns the end bound of a period.
 */
@ExpressionDescription(
  usage = "_FUNC_(period) - Returns the end bound of a `period` expression.",
  extended = """
    Examples:
      > SELECT _FUNC_(PERIOD('2016-07-25', '2016-07-31'));
       2016-07-31
  """)
case class PeriodEnd(period: Expression)
  extends UnaryExpression with ExpectsInputTypes {

  override def child: Expression = period

  override def inputTypes: Seq[AbstractDataType] = Seq(PeriodType)

  override def dataType: DataType = child.dataType.asInstanceOf[PeriodType].elementType

  override def nullSafeEval(input: Any): Any = {
    input.asInstanceOf[ArrayData].get(1, dataType)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, p => {
      s"""${ev.value} = ${ctx.getValue(p, dataType, String.valueOf(1))};"""
    })
  }

  override def prettyName: String = "end"
}


// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(period) - Returns the last value of the Period argument (that is, the ending bound minus one granule of the element type of the argument).",
  extended = """
    Examples:
      > SELECT _FUNC_(PERIOD('2016-08-02', '2016-08-30'));
       2016-08-29
  """)
// scalastyle:on line.size.limit
case class PeriodLast(child: Expression)
  extends UnaryExpression with ExpectsInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(PeriodType)

  override def checkInputDataTypes(): TypeCheckResult = {
    TypeCheckResult.TypeCheckSuccess
  }

  override def dataType: DataType = child.dataType.asInstanceOf[PeriodType].elementType

  override def nullSafeEval(input: Any): Any = {
    val result = PeriodUtils.end(input) - 1

    dataType match {
      case DateType => result.toInt
      case TimestampType => result
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, eval => {
      s"""${ev.value} = ${ctx.getValue(eval, dataType, String.valueOf(1))} - 1;"""
    })
  }

  override def prettyName: String = "last"
}

@ExpressionDescription(
  usage = "_FUNC_(period1, period2) - Returns true if `period1` equals `period2`.",
  extended = """
    Examples:
      > SELECT _FUNC_(PERIOD('2016-07-25', '2016-09-30'), PERIOD('2016-07-25', '2016-09-30'));
       TRUE
  """)
case class PeriodEquals(left: Expression, right: Expression)
  extends BinaryExpression with ExpectsInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(PeriodType, PeriodType)

  override def checkInputDataTypes(): TypeCheckResult = {
    TypeUtils.checkForSameTypeInputExpr(
      children.map(_.dataType.asInstanceOf[PeriodType].elementType),
      s"function $prettyName")
  }

  override def dataType: DataType = BooleanType

  private lazy val elementType = left.dataType.asInstanceOf[PeriodType].elementType

  override def nullSafeEval(input1: Any, input2: Any): Any = {
    PeriodUtils.begin(input1) == PeriodUtils.begin(input2) &&
      PeriodUtils.end(input1) == PeriodUtils.end(input2)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, (eval1, eval2) => {
      s"""
        ${ev.value} =
          ${ctx.getValue(eval1, elementType, String.valueOf(0))} ==
            ${ctx.getValue(eval2, elementType, String.valueOf(0))} &&
          ${ctx.getValue(eval1, elementType, String.valueOf(1))} ==
            ${ctx.getValue(eval2, elementType, String.valueOf(1))};
      """
    })
  }

  override def prettyName: String = "equals"
}

@ExpressionDescription(
  usage = "_FUNC_(period1, period2) - Returns true if `period1` contains `period2`.",
  extended = """
    Examples:
      > SELECT _FUNC_(PERIOD('2016-07-25', '2016-09-30'), PERIOD('2016-07-31', '2016-08-30'));
       TRUE
  """)
case class PeriodContains(left: Expression, right: Expression)
  extends BinaryExpression with ExpectsInputTypes {

  override def inputTypes: Seq[AbstractDataType] =
    Seq(PeriodType, TypeCollection(PeriodType, DateType, TimestampType))

  override def checkInputDataTypes(): TypeCheckResult = {
    (left.dataType, right.dataType) match {
      case (p1: PeriodType, p2: PeriodType) =>
        if (p1.elementType.sameType(p2.elementType)) {
          TypeCheckResult.TypeCheckSuccess
        } else {
          TypeCheckResult.TypeCheckFailure(
            s"Element types of periods ${p1.simpleString} and ${p2.simpleString} do not match"
          )
        }

      case (t @ (DateType | TimestampType), p: PeriodType) =>
          TypeCheckResult.TypeCheckFailure(
            s"First argument must be of type ${p.simpleString}, not ${t.simpleString}"
          )

      case (p: PeriodType, t @ (DateType | TimestampType)) =>
        if (p.elementType.sameType(t)) {
          TypeCheckResult.TypeCheckSuccess
        } else {
          TypeCheckResult.TypeCheckFailure(
            s"The element type of period ${p.simpleString} is not comparable to" +
                s"type ${t.simpleString}"
          )
        }
    }
  }

  override def dataType: DataType = BooleanType

  override def nullSafeEval(input1: Any, input2: Any): Any = {
    (left.dataType, right.dataType) match {
      case (_: PeriodType, (DateType | TimestampType)) =>
        PeriodUtils.begin(input1) <= PeriodUtils.toLong(input2) &&
          PeriodUtils.end(input1) > PeriodUtils.toLong(input2)

      case (_: PeriodType, _: PeriodType) =>
        PeriodUtils.begin(input1) <= PeriodUtils.begin(input2) &&
          PeriodUtils.end(input1) >= PeriodUtils.end(input2)
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, (eval1, eval2) => {
      (left.dataType, right.dataType) match {
        case (t: PeriodType, (DateType| TimestampType)) =>
          s"""
            ${ev.value} =
              ${ctx.getValue(eval1, t.elementType, String.valueOf(0))} <= $eval2 &&
              ${ctx.getValue(eval1, t.elementType, String.valueOf(1))} > $eval2;
          """
        case (t: PeriodType, _: PeriodType) =>
          s"""
            ${ev.value} =
              ${ctx.getValue(eval1, t.elementType, String.valueOf(0))} <=
                ${ctx.getValue(eval2, t.elementType, String.valueOf(0))} &&
              ${ctx.getValue(eval1, t.elementType, String.valueOf(1))} >=
                ${ctx.getValue(eval2, t.elementType, String.valueOf(1))};
          """
      }
    })
  }

  override def prettyName: String = "contains"
}

@ExpressionDescription(
  usage = """
    _FUNC_(period1, period2) - Returns the portion of the period expressions that is
    common between the period expressions if they overlap.
  """,
  extended = """
    Examples:
      > SELECT _FUNC_(PERIOD('2005-02-03', '2007-02-03'), PERIOD('2004-02-03', '2006-02-03'));
       PERIOD('2005-02-03', '2006-02-03')
  """)
case class PeriodIntersect(left: Expression, right: Expression)
  extends BinaryExpression with ExpectsInputTypes {

  override def nullable: Boolean = true

  override def inputTypes: Seq[AbstractDataType] = Seq(PeriodType, PeriodType)

  override def checkInputDataTypes(): TypeCheckResult = {
    (left.dataType, right.dataType) match {
      case (_: PeriodType, _: PeriodType) =>
        TypeUtils.checkForSameTypeInputExpr(
          children.map(_.dataType.asInstanceOf[PeriodType].elementType),
          s"function $prettyName")

      case (t @ (DateType | TimestampType), p: PeriodType) =>
        TypeCheckResult.TypeCheckFailure(
          s"First argument must be of type ${p.simpleString}, not ${t.simpleString}"
        )

      case (p: PeriodType, t @ (DateType | TimestampType)) =>
        TypeCheckResult.TypeCheckFailure(
          s"Second argument must be of type ${p.simpleString}, not ${t.simpleString}"
        )

      case _ =>
        TypeCheckResult.TypeCheckFailure(s"Arguments must be period expressions")
    }
  }

  override def dataType: DataType = PeriodType(
    left.dataType.asInstanceOf[PeriodType].elementType
  )

  private lazy val elementType = left.dataType.asInstanceOf[PeriodType].elementType

  override def nullSafeEval(input1: Any, input2: Any): Any = {
    val s1 = PeriodUtils.begin(input1)
    val e1 = PeriodUtils.end(input1)
    val s2 = PeriodUtils.begin(input2)
    val e2 = PeriodUtils.end(input2)

    val overlaps = s1.max(s2) <= e1.min(e2)

    if (overlaps) {
      val begin = if (s1 >= s2) s1 else s2
      val end = if (e1 <= e2) e1 else e2

      elementType match {
        case DateType => new GenericArrayData(Seq(begin.toInt, end.toInt))
        case _ => new GenericArrayData(Seq(begin, end))
      }
    } else {
      null
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, (eval1, eval2) => {
      val overlaps = ctx.freshName("overlaps")

      val s1 = ctx.freshName("s1")
      val e1 = ctx.freshName("e1")
      val s2 = ctx.freshName("s2")
      val e2 = ctx.freshName("e2")

      val genericArrayClass = classOf[GenericArrayData].getName
      val arrayName = ctx.freshName("array")

      ctx.addMutableState("Object[]", arrayName, s"$arrayName = new Object[2];")

      val javaType = ctx.javaType(elementType)

      s"""
        $javaType $s1 = ${ctx.getValue(eval1, elementType, String.valueOf(0))};
        $javaType $e1 = ${ctx.getValue(eval1, elementType, String.valueOf(1))};
        $javaType $s2 = ${ctx.getValue(eval2, elementType, String.valueOf(0))};
        $javaType $e2 = ${ctx.getValue(eval2, elementType, String.valueOf(1))};

        final boolean $overlaps = java.lang.Math.max($s1, $s2) <= java.lang.Math.min($e1, $e2);

        if ($overlaps) {
          $arrayName[0] = $s1 >= $s2 ? $s1 : $s2;
          $arrayName[1] = $e1 <= $e2 ? $e1 : $e2;

          ${ev.value} = new $genericArrayClass($arrayName);
        } else {
          ${ev.isNull} = true;
        }
      """
    })
  }

  override def prettyName: String = "intersect"
}

@ExpressionDescription(
  usage =
    """_FUNC_(period1, period2) - Returns the portion of the first Period expression
    that exists before the beginning of the second Period expression when the Period expressions overlap.
    """,
  extended = """
    Examples:
      > SELECT _FUNC_(PERIOD('2016-07-25', '2016-08-02'), PERIOD('2016-07-31', '2016-08-30'));
       PERIOD('2016-07-25', '2016-07-31')
  """)
case class PeriodLDiff(left: Expression, right: Expression)
  extends BinaryExpression with ExpectsInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(PeriodType, PeriodType)

  override def nullable: Boolean = true

  override def checkInputDataTypes(): TypeCheckResult = {
    TypeUtils.checkForSameTypeInputExpr(
      children.map(_.dataType.asInstanceOf[PeriodType].elementType),
      s"function $prettyName")
  }

  private lazy val elementType = left.dataType.asInstanceOf[PeriodType].elementType

  override def dataType: DataType = PeriodType(elementType)

  override def nullSafeEval(input1: Any, input2: Any): Any = {
    val s1 = PeriodUtils.begin(input1)
    val e1 = PeriodUtils.end(input1)
    val s2 = PeriodUtils.begin(input2)
    val e2 = PeriodUtils.end(input2)

    val overlaps = s1.max(s2) <= e1.min(e2)

    if (overlaps && s1 < s2) {
      elementType match {
        case DateType => new GenericArrayData(Seq(s1.toInt, s2.toInt))
        case _ => new GenericArrayData(Seq(s1, s2))
      }
    } else {
      null
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, (eval1, eval2) => {
      val genericArrayClass = classOf[GenericArrayData].getName
      val arrayName = ctx.freshName("array")
      val arrayDataName = ctx.freshName("arrayData")

      val s1 = ctx.freshName("s1")
      val e1 = ctx.freshName("e1")
      val s2 = ctx.freshName("s2")
      val e2 = ctx.freshName("e2")

      val overlaps = ctx.freshName("overlaps")

      ctx.addMutableState("Object[]", arrayName, s"$arrayName = new Object[2];")

      val javaType = ctx.javaType(elementType)

      s"""
        $javaType $s1 = ${ctx.getValue(eval1, elementType, String.valueOf(0))};
        $javaType $e1 = ${ctx.getValue(eval1, elementType, String.valueOf(1))};
        $javaType $s2 = ${ctx.getValue(eval2, elementType, String.valueOf(0))};
        $javaType $e2 = ${ctx.getValue(eval2, elementType, String.valueOf(1))};

        final boolean $overlaps = java.lang.Math.max($s1, $s2) <= java.lang.Math.min($e1, $e2);
        if ($overlaps && $s1 < $s2) {
          final ArrayData $arrayDataName = null;

          $arrayName[0] = $s1;
          $arrayName[1] = $s2;

          ${ev.value} = new $genericArrayClass($arrayName);
        } else {
          ${ev.isNull} = true;
        }
      """
    })
  }

  override def prettyName: String = "ldiff"
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """_FUNC_(period1, period2) - Returns the portion of the first Period expression that exists from the end of the second Period expression when the Period expressions overlap.""",
  extended = """
    Examples:
      > SELECT _FUNC_(PERIOD('2016-07-25', '2016-08-02'), PERIOD('2016-07-31', '2016-08-30'));
       PERIOD('2016-07-25', '2016-07-31')
  """)
// scalastyle:on line.size.limit
case class PeriodRDiff(left: Expression, right: Expression)
  extends BinaryExpression with ExpectsInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(PeriodType, PeriodType)

  override def nullable: Boolean = true

  override def checkInputDataTypes(): TypeCheckResult = {
    TypeUtils.checkForSameTypeInputExpr(
      children.map(_.dataType.asInstanceOf[PeriodType].elementType),
      s"function $prettyName")
  }

  private lazy val elementType = left.dataType.asInstanceOf[PeriodType].elementType

  override def dataType: DataType = PeriodType(elementType)

  override def nullSafeEval(input1: Any, input2: Any): Any = {
    val s1 = PeriodUtils.begin(input1)
    val e1 = PeriodUtils.end(input1)
    val s2 = PeriodUtils.begin(input2)
    val e2 = PeriodUtils.end(input2)

    val overlaps = s1.max(s2) <= e1.min(e2)

    if (overlaps && e1 > e2) {
      elementType match {
        case DateType => new GenericArrayData(Seq(e2.toInt, e1.toInt))
        case _ => new GenericArrayData(Seq(e2, e1))
      }
    } else {
      null
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, (eval1, eval2) => {
      val genericArrayClass = classOf[GenericArrayData].getName
      val arrayName = ctx.freshName("array")
      val arrayDataName = ctx.freshName("arrayData")

      val s1 = ctx.freshName("s1")
      val e1 = ctx.freshName("e1")
      val s2 = ctx.freshName("s2")
      val e2 = ctx.freshName("e2")

      val overlaps = ctx.freshName("overlaps")

      ctx.addMutableState("Object[]", arrayName, s"$arrayName = new Object[2];")

      val javaType = ctx.javaType(elementType)

      s"""
        $javaType $s1 = ${ctx.getValue(eval1, elementType, String.valueOf(0))};
        $javaType $e1 = ${ctx.getValue(eval1, elementType, String.valueOf(1))};
        $javaType $s2 = ${ctx.getValue(eval2, elementType, String.valueOf(0))};
        $javaType $e2 = ${ctx.getValue(eval2, elementType, String.valueOf(1))};

        final boolean $overlaps = java.lang.Math.max($s1, $s2) <= java.lang.Math.min($e1, $e2);
        if ($overlaps && $e1 > $e2) {
          final ArrayData $arrayDataName = null;

          $arrayName[0] = $e2;
          $arrayName[1] = $e1;

          ${ev.value} = new $genericArrayClass($arrayName);
        } else {
          ${ev.isNull} = true;
        }
      """
    })
  }

  override def prettyName: String = "rdiff"
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """_FUNC_(period1, period2) - Returns a Period value that is the combination of the two Period expressions if the Period expressions overlap or meet.""",
  extended = """
    Examples:
      > SELECT _FUNC_(PERIOD('2016-07-25', '2016-08-02'), PERIOD('2016-07-31', '2016-08-30'));
       PERIOD('2016-07-25', '2016-07-31')
  """)
// scalastyle:on line.size.limit
case class PeriodNormalize(left: Expression, right: Expression)
  extends BinaryExpression with ExpectsInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(PeriodType, PeriodType)

  override def nullable: Boolean = true

  override def checkInputDataTypes(): TypeCheckResult = {
    TypeUtils.checkForSameTypeInputExpr(
      children.map(_.dataType.asInstanceOf[PeriodType].elementType),
      s"function $prettyName")
  }

  private lazy val elementType = left.dataType.asInstanceOf[PeriodType].elementType

  override def dataType: DataType = PeriodType(elementType)

  override def nullSafeEval(input1: Any, input2: Any): Any = {
    val s1 = PeriodUtils.begin(input1)
    val e1 = PeriodUtils.end(input1)
    val s2 = PeriodUtils.begin(input2)
    val e2 = PeriodUtils.end(input2)

    val overlaps = s1.max(s2) <= e1.min(e2)
    val meets = e1 == s2 || e2 == s1

    if (overlaps || meets) {
      if ((s1 >= s2 && s1 <= e2) || (s2 >= s1 && s2 <= e1)) {
        val min_begin = s1.min(s2)
        val max_end = e1.max(e2)
        elementType match {
          case DateType => new GenericArrayData(Seq(min_begin.toInt, max_end.toInt))
          case _ => new GenericArrayData(Seq(min_begin, max_end))
        }
      } else {
        null
      }
    } else {
      null
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, (eval1, eval2) => {
      val genericArrayClass = classOf[GenericArrayData].getName
      val arrayName = ctx.freshName("array")
      val arrayDataName = ctx.freshName("arrayData")

      val s1 = ctx.freshName("s1")
      val e1 = ctx.freshName("e1")
      val s2 = ctx.freshName("s2")
      val e2 = ctx.freshName("e2")

      val overlaps = ctx.freshName("overlaps")
      val meets = ctx.freshName("meets")

      ctx.addMutableState("Object[]", arrayName, s"$arrayName = new Object[2];")

      val javaType = ctx.javaType(elementType)

      s"""
        $javaType $s1 = ${ctx.getValue(eval1, elementType, String.valueOf(0))};
        $javaType $e1 = ${ctx.getValue(eval1, elementType, String.valueOf(1))};
        $javaType $s2 = ${ctx.getValue(eval2, elementType, String.valueOf(0))};
        $javaType $e2 = ${ctx.getValue(eval2, elementType, String.valueOf(1))};

        final boolean $overlaps = java.lang.Math.max($s1, $s2) <= java.lang.Math.min($e1, $e2);
        final boolean $meets = $e1 == $s2 || $e2 == $s1;

        if ($overlaps || $meets) {
          if (($s1 >= $s2 && $s1 <= $e2) || ($s2 >= $s1 && $s2 <= $e1)) {
            final ArrayData $arrayDataName = null;

            $arrayName[0] = java.lang.Math.min($s1, $s2);
            $arrayName[1] = java.lang.Math.max($e1, $e2);

            ${ev.value} = new $genericArrayClass($arrayName);
          } else {
            ${ev.isNull} = true;
          }
        } else {
          ${ev.isNull} = true;
        }
      """
    })
  }

  override def prettyName: String = "normalize"
}

@ExpressionDescription(
  usage = "_FUNC_(period1, period2) - Returns true if `period1` and `period2` meet.",
  extended = """
    Examples:
      > SELECT _FUNC_(PERIOD('2016-07-25', '2016-07-31'), PERIOD('2016-07-31', '2016-08-30'));
       TRUE
  """)
case class PeriodMeets(left: Expression, right: Expression)
  extends BinaryExpression with ExpectsInputTypes {

  override def inputTypes: Seq[AbstractDataType] =
    Seq(
      TypeCollection(PeriodType, DateType, TimestampType),
      TypeCollection(PeriodType, DateType, TimestampType)
    )

  override def checkInputDataTypes(): TypeCheckResult = {
    (left.dataType, right.dataType) match {
      case (p1: PeriodType, p2: PeriodType) =>
        if (p1.elementType.sameType(p2.elementType)) {
          TypeCheckResult.TypeCheckSuccess
        } else {
          TypeCheckResult.TypeCheckFailure(
            s"Element types of periods ${p1.simpleString} and ${p2.simpleString} do not match"
          )
        }

      case (t @ (DateType | TimestampType), p: PeriodType) =>
        TypeCheckResult.TypeCheckFailure(
          s"First argument must be of type ${p.simpleString}, not ${t.simpleString}"
        )

      case (p: PeriodType, t @ (DateType | TimestampType)) =>
        if (p.elementType.sameType(t)) {
          TypeCheckResult.TypeCheckSuccess
        } else {
          TypeCheckResult.TypeCheckFailure(
            s"The element type of period ${p.simpleString} is not comparable to" +
              s"type ${t.simpleString}"
          )
        }
    }
  }

  override def dataType: DataType = BooleanType

  override def nullSafeEval(input1: Any, input2: Any): Any = {
    (left.dataType, right.dataType) match {
      case (_: PeriodType, (DateType | TimestampType)) =>
        PeriodUtils.end(input1) == PeriodUtils.toLong(input2) ||
          (PeriodUtils.toLong(input2) + 1) == PeriodUtils.begin(input1)

      case ((DateType | TimestampType), _: PeriodType) =>
        PeriodUtils.end(input2) == PeriodUtils.toLong(input1) ||
          (PeriodUtils.toLong(input1) + 1) == PeriodUtils.begin(input2)

      case (_: PeriodType, _: PeriodType) =>
        PeriodUtils.end(input1) == PeriodUtils.begin(input2) ||
          PeriodUtils.end(input2) == PeriodUtils.begin(input1)
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, (eval1, eval2) => {
      (left.dataType, right.dataType) match {
        case (t: PeriodType, (DateType| TimestampType)) =>
          s"""
            ${ev.value} =
              $eval2 == ${ctx.getValue(eval1, t.elementType, String.valueOf(1))} ||
                ($eval2 + 1) == ${ctx.getValue(eval1, t.elementType, String.valueOf(0))};
          """
        case ((DateType | TimestampType), t: PeriodType) =>
          s"""
            ${ev.value} =
              $eval1 == ${ctx.getValue(eval2, t.elementType, String.valueOf(1))} ||
                ($eval1 + 1) == ${ctx.getValue(eval2, t.elementType, String.valueOf(0))};
          """
        case (t: PeriodType, _: PeriodType) =>
          s"""
            ${ev.value} =
              ${ctx.getValue(eval1, t.elementType, String.valueOf(1))} ==
                ${ctx.getValue(eval2, t.elementType, String.valueOf(0))} ||
              ${ctx.getValue(eval2, t.elementType, String.valueOf(1))} ==
                ${ctx.getValue(eval1, t.elementType, String.valueOf(0))};
          """
      }
    })
  }

  override def prettyName: String = "meets"
}

@ExpressionDescription(
  usage = "_FUNC_(period1, period2) - Returns true if `period1` and `period2` overlap.",
  extended = """
    Examples:
      > SELECT _FUNC_(PERIOD('2016-07-25', '2016-07-31'), PERIOD('2016-07-31', '2016-08-30'));
       TRUE
  """)
case class PeriodOverlaps(left: Expression, right: Expression)
  extends BinaryExpression with ExpectsInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(PeriodType, PeriodType)

  override def checkInputDataTypes(): TypeCheckResult = {
    TypeUtils.checkForSameTypeInputExpr(
      children.map(_.dataType.asInstanceOf[PeriodType].elementType),
      s"function $prettyName")
  }

  override def dataType: DataType = BooleanType

  override def nullSafeEval(input1: Any, input2: Any): Any = {
    PeriodUtils.begin(input1).max(PeriodUtils.begin(input2)) <=
      PeriodUtils.end(input1).min(PeriodUtils.end(input2))
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, (eval1, eval2) => {
      val elementType = left.dataType.asInstanceOf[PeriodType].elementType
      s"""
        ${ev.value} =
          java.lang.Math.max(
            ${ctx.getValue(eval1, elementType, String.valueOf(0))},
            ${ctx.getValue(eval2, elementType, String.valueOf(0))}) <=
          java.lang.Math.min(
            ${ctx.getValue(eval1, elementType, String.valueOf(1))},
            ${ctx.getValue(eval2, elementType, String.valueOf(1))});
      """
    })
  }

  override def prettyName: String = "overlaps"
}

@ExpressionDescription(
  usage = "_FUNC_(period1, period2) - Returns true if `period1` precedes `period2`.",
  extended = """
    Examples:
      > SELECT _FUNC_(PERIOD('2016-07-25', '2016-07-31'), PERIOD('2016-08-02', '2016-08-30'));
       true
  """)
case class PeriodPrecedes(left: Expression, right: Expression)
  extends BinaryExpression with ExpectsInputTypes {

  override def inputTypes: Seq[AbstractDataType] =
    Seq(
      TypeCollection(PeriodType, DateType, TimestampType),
      TypeCollection(PeriodType, DateType, TimestampType)
    )

  override def checkInputDataTypes(): TypeCheckResult = {
    (left.dataType, right.dataType) match {
      case (p1: PeriodType, p2: PeriodType) =>
        if (p1.elementType.sameType(p2.elementType)) {
          TypeCheckResult.TypeCheckSuccess
        } else {
          TypeCheckResult.TypeCheckFailure(
            s"Element types of periods ${p1.simpleString} and ${p2.simpleString} do not match"
          )
        }

      case (t @ (DateType | TimestampType), p: PeriodType) =>
        if (t.sameType(p.elementType)) {
          TypeCheckResult.TypeCheckSuccess
        } else {
          TypeCheckResult.TypeCheckFailure(
            s"The type ${t.simpleString} is not comparable to the element type of " +
              s"period ${p.simpleString}"
          )
        }

      case (p: PeriodType, t @ (DateType | TimestampType)) =>
        if (p.elementType.sameType(t)) {
          TypeCheckResult.TypeCheckSuccess
        } else {
          TypeCheckResult.TypeCheckFailure(
            s"The element type of period ${p.simpleString} is not comparable to" +
              s"type ${t.simpleString}"
          )
        }
    }
  }

  override def dataType: DataType = BooleanType

  override def nullSafeEval(input1: Any, input2: Any): Any = {
    (left.dataType, right.dataType) match {
      case (_: PeriodType, (DateType | TimestampType)) =>
        PeriodUtils.end(input1) <= PeriodUtils.toLong(input2)

      case (DateType | TimestampType, _: PeriodType) =>
        PeriodUtils.toLong(input1) < PeriodUtils.begin(input2)

      case (_: PeriodType, _: PeriodType) =>
        PeriodUtils.end(input1) <= PeriodUtils.begin(input2)
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, (eval1, eval2) => {
      (left.dataType, right.dataType) match {
        case (t: PeriodType, (DateType | TimestampType)) =>
          s"${ev.value} = ${ctx.getValue(eval1, t.elementType, String.valueOf(1))} <= $eval2;"

        case ((DateType | TimestampType), t: PeriodType) =>
          s"${ev.value} = $eval1 < ${ctx.getValue(eval2, t.elementType, String.valueOf(0))};"

        case (t: PeriodType, _: PeriodType) =>
          s"""
            ${ev.value} = ${ctx.getValue(eval1, t.elementType, String.valueOf(1))} <=
              ${ctx.getValue(eval2, t.elementType, String.valueOf(0))};
          """
      }
    })
  }

  override def prettyName: String = "precedes"
}

@ExpressionDescription(
  usage = "_FUNC_(period1, period2) - Returns true if `period1` succedes `period2`.",
  extended = """
    Examples:
      > SELECT _FUNC_(PERIOD('2016-08-02', '2016-08-30'), PERIOD('2016-07-25', '2016-07-31'));
       true
  """)
case class PeriodSucceeds(left: Expression, right: Expression)
  extends BinaryExpression with ExpectsInputTypes {

  override def inputTypes: Seq[AbstractDataType] =
    Seq(
      TypeCollection(PeriodType, DateType, TimestampType),
      TypeCollection(PeriodType, DateType, TimestampType)
    )

  override def checkInputDataTypes(): TypeCheckResult = {
    (left.dataType, right.dataType) match {
      case (p1: PeriodType, p2: PeriodType) =>
        if (p1.elementType.sameType(p2.elementType)) {
          TypeCheckResult.TypeCheckSuccess
        } else {
          TypeCheckResult.TypeCheckFailure(
            s"Element types of periods ${p1.simpleString} and ${p2.simpleString} do not match"
          )
        }

      case (t @ (DateType | TimestampType), p: PeriodType) =>
        if (t.sameType(p.elementType)) {
          TypeCheckResult.TypeCheckSuccess
        } else {
          TypeCheckResult.TypeCheckFailure(
            s"The type ${t.simpleString} is not comparable to the element type of " +
              s"period ${p.simpleString}"
          )
        }

      case (p: PeriodType, t @ (DateType | TimestampType)) =>
        if (p.elementType.sameType(t)) {
          TypeCheckResult.TypeCheckSuccess
        } else {
          TypeCheckResult.TypeCheckFailure(
            s"The element type of period ${p.simpleString} is not comparable to" +
              s"type ${t.simpleString}"
          )
        }
    }
  }

  override def dataType: DataType = BooleanType

  lazy val elementType: DataType = left.dataType.asInstanceOf[PeriodType].elementType

  override def nullSafeEval(input1: Any, input2: Any): Any = {
    (left.dataType, right.dataType) match {
      case (_: PeriodType, (DateType | TimestampType)) =>
        PeriodUtils.begin(input1) > PeriodUtils.toLong(input2)

      case ((DateType | TimestampType), _: PeriodType) =>
        PeriodUtils.toLong(input1) >= PeriodUtils.end(input2)

      case (_: PeriodType, _: PeriodType) =>
        PeriodUtils.begin(input1) >= PeriodUtils.end(input2)
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, (eval1, eval2) => {
      (left.dataType, right.dataType) match {
        case (t: PeriodType, (DateType | TimestampType)) =>
          s"${ev.value} = ${ctx.getValue(eval1, t.elementType, String.valueOf(0))} > $eval2;"

        case ((DateType | TimestampType), t: PeriodType) =>
          s"${ev.value} = $eval1 >= ${ctx.getValue(eval2, t.elementType, String.valueOf(1))};"

        case (t: PeriodType, _: PeriodType) =>
          s"""
            ${ev.value} = ${ctx.getValue(eval1, t.elementType, String.valueOf(0))} >=
              ${ctx.getValue(eval2, t.elementType, String.valueOf(1))};
          """
      }
    })
  }

  override def prettyName: String = "succeeds"
}


@ExpressionDescription(
  usage = "_FUNC_(period1, period2) - Returns true if `period1` immediately precedes `period2`.",
  extended = """
    Examples:
      > SELECT _FUNC_(PERIOD('2016-07-25', '2016-08-02'), PERIOD('2016-08-02', '2016-08-30'));
       true
  """)
case class PeriodImmediatelyPrecedes(left: Expression, right: Expression)
  extends BinaryExpression with ExpectsInputTypes {

  override def inputTypes: Seq[AbstractDataType] =
    Seq(
      TypeCollection(PeriodType, DateType, TimestampType),
      TypeCollection(PeriodType, DateType, TimestampType)
    )

  override def checkInputDataTypes(): TypeCheckResult = {
    (left.dataType, right.dataType) match {
      case (p1: PeriodType, p2: PeriodType) =>
        if (p1.elementType.sameType(p2.elementType)) {
          TypeCheckResult.TypeCheckSuccess
        } else {
          TypeCheckResult.TypeCheckFailure(
            s"Element types of periods ${p1.simpleString} and ${p2.simpleString} do not match"
          )
        }

      case (t @ (DateType | TimestampType), p: PeriodType) =>
        if (t.sameType(p.elementType)) {
          TypeCheckResult.TypeCheckSuccess
        } else {
          TypeCheckResult.TypeCheckFailure(
            s"The type ${t.simpleString} is not comparable to the element type of " +
              s"period ${p.simpleString}"
          )
        }

      case (p: PeriodType, t @ (DateType | TimestampType)) =>
        if (p.elementType.sameType(t)) {
          TypeCheckResult.TypeCheckSuccess
        } else {
          TypeCheckResult.TypeCheckFailure(
            s"The element type of period ${p.simpleString} is not comparable to" +
              s"type ${t.simpleString}"
          )
        }
    }
  }

  override def dataType: DataType = BooleanType

  override def nullSafeEval(input1: Any, input2: Any): Any = {
    (left.dataType, right.dataType) match {
      case (_: PeriodType, (DateType | TimestampType)) =>
        PeriodUtils.end(input1) <= PeriodUtils.toLong(input2)

      case (DateType | TimestampType, _: PeriodType) =>
        PeriodUtils.toLong(input1) < PeriodUtils.begin(input2)

      case (_: PeriodType, _: PeriodType) =>
        PeriodUtils.end(input1) == PeriodUtils.begin(input2)
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, (eval1, eval2) => {
      (left.dataType, right.dataType) match {
        case (t: PeriodType, (DateType | TimestampType)) =>
          s"${ev.value} = ${ctx.getValue(eval1, t.elementType, String.valueOf(1))} <= $eval2;"

        case ((DateType | TimestampType), t: PeriodType) =>
          s"${ev.value} = $eval1 < ${ctx.getValue(eval2, t.elementType, String.valueOf(0))};"

        case (t: PeriodType, _: PeriodType) =>
          s"""
            ${ev.value} = ${ctx.getValue(eval1, t.elementType, String.valueOf(1))} =
              ${ctx.getValue(eval2, t.elementType, String.valueOf(0))};
          """
      }
    })
  }

  override def prettyName: String = "precedes"
}

@ExpressionDescription(
  usage = "_FUNC_(period1, period2) - Returns true if `period1` immediately succeeds `period2`.",
  extended = """
    Examples:
      > SELECT _FUNC_(PERIOD('2016-07-31', '2016-08-30'), PERIOD('2016-07-25', '2016-07-31'));
       true
  """)
case class PeriodImmediatelySucceeds(left: Expression, right: Expression)
  extends BinaryExpression with ExpectsInputTypes {

  override def inputTypes: Seq[AbstractDataType] =
    Seq(
      TypeCollection(PeriodType, DateType, TimestampType),
      TypeCollection(PeriodType, DateType, TimestampType)
    )

  override def checkInputDataTypes(): TypeCheckResult = {
    (left.dataType, right.dataType) match {
      case (p1: PeriodType, p2: PeriodType) =>
        if (p1.elementType.sameType(p2.elementType)) {
          TypeCheckResult.TypeCheckSuccess
        } else {
          TypeCheckResult.TypeCheckFailure(
            s"Element types of periods ${p1.simpleString} and ${p2.simpleString} do not match"
          )
        }

      case (t @ (DateType | TimestampType), p: PeriodType) =>
        if (t.sameType(p.elementType)) {
          TypeCheckResult.TypeCheckSuccess
        } else {
          TypeCheckResult.TypeCheckFailure(
            s"The type ${t.simpleString} is not comparable to the element type of " +
              s"period ${p.simpleString}"
          )
        }

      case (p: PeriodType, t @ (DateType | TimestampType)) =>
        if (p.elementType.sameType(t)) {
          TypeCheckResult.TypeCheckSuccess
        } else {
          TypeCheckResult.TypeCheckFailure(
            s"The element type of period ${p.simpleString} is not comparable to" +
              s"type ${t.simpleString}"
          )
        }
    }
  }

  override def dataType: DataType = BooleanType

  lazy val elementType: DataType = left.dataType.asInstanceOf[PeriodType].elementType

  override def nullSafeEval(input1: Any, input2: Any): Any = {
    (left.dataType, right.dataType) match {
      case (_: PeriodType, (DateType | TimestampType)) =>
        PeriodUtils.begin(input1) > PeriodUtils.toLong(input2)

      case ((DateType | TimestampType), _: PeriodType) =>
        PeriodUtils.toLong(input1) >= PeriodUtils.end(input2)

      case (_: PeriodType, _: PeriodType) =>
        PeriodUtils.begin(input1) == PeriodUtils.end(input2)
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, (eval1, eval2) => {
      (left.dataType, right.dataType) match {
        case (t: PeriodType, (DateType | TimestampType)) =>
          s"${ev.value} = ${ctx.getValue(eval1, t.elementType, String.valueOf(0))} > $eval2;"

        case ((DateType | TimestampType), t: PeriodType) =>
          s"${ev.value} = $eval1 >= ${ctx.getValue(eval2, t.elementType, String.valueOf(1))};"

        case (t: PeriodType, _: PeriodType) =>
          s"""
            ${ev.value} = ${ctx.getValue(eval1, t.elementType, String.valueOf(0))} >=
              ${ctx.getValue(eval2, t.elementType, String.valueOf(1))};
          """
      }
    })
  }

  override def prettyName: String = "succeeds"
}

object PeriodUtils {

  def toLong(v: Any): Long = {
    v match {
      case v: SQLDate => v.toLong
      case v: SQLTimestamp => v
    }
  }

  def begin(p: Any): Long = {
    toLong(p.asInstanceOf[GenericArrayData].array(0))
  }

  def end(p: Any): Long = {
    toLong(p.asInstanceOf[GenericArrayData].array(1))
  }

  def asType(v: Any, dt: DataType): Any = {

  }
}