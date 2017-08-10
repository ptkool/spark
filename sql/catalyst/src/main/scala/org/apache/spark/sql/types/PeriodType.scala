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

package org.apache.spark.sql.types

class PeriodType(elementType: DataType) extends ArrayType(elementType, false) {

  override def defaultSize: Int = 2 * elementType.defaultSize

  override def simpleString: String = s"period(${elementType.simpleString})"

  override def catalogString: String = s"period${elementType.catalogString})"

  override def sql: String = s"PERIOD(${elementType.sql}"

  override private[spark] def asNullable: PeriodType = this
}

case object PeriodType extends AbstractDataType {

  override private[sql] def defaultConcreteType: DataType = new PeriodType(TimestampType)

  override private[sql] def acceptsType(other: DataType): Boolean = {
    other.isInstanceOf[PeriodType]
  }

  override private[sql] def simpleString: String = "period"

  def apply(): PeriodType = new PeriodType(TimestampType)

  def apply(elementType: DataType): PeriodType = new PeriodType(elementType)

  def unapply(arg: PeriodType): Option[DataType] = Some(arg.elementType)
}
