

/*******************************************************************************

 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.apache.arrow.vector.complex.impl;


import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import com.google.common.collect.Lists;
import com.google.common.collect.ObjectArrays;
import com.google.common.base.Charsets;
import com.google.common.collect.ObjectArrays;

import com.google.common.base.Preconditions;
import io.netty.buffer.*;

import org.apache.commons.lang3.ArrayUtils;

import org.apache.arrow.memory.*;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.Types.*;
import org.apache.arrow.vector.types.*;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.holders.*;
import org.apache.arrow.vector.util.*;
import org.apache.arrow.vector.complex.*;
import org.apache.arrow.vector.complex.reader.*;
import org.apache.arrow.vector.complex.impl.*;
import org.apache.arrow.vector.complex.writer.*;
import org.apache.arrow.vector.complex.writer.BaseWriter.MapWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.arrow.vector.util.JsonStringArrayList;

import java.util.Arrays;
import java.util.Random;
import java.util.List;

import java.io.Closeable;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.math.BigDecimal;
import java.math.BigInteger;

import org.joda.time.DateTime;
import org.joda.time.Period;







/*
 * A FieldWriter which delegates calls to another FieldWriter. The delegate FieldWriter can be promoted to a new type
 * when necessary. Classes that extend this class are responsible for handling promotion.
 *
 * This class is generated using freemarker and the AbstractPromotableFieldWriter.java template.
 *
 */
@SuppressWarnings("unused")
abstract class AbstractPromotableFieldWriter extends AbstractFieldWriter {
  AbstractPromotableFieldWriter(FieldWriter parent) {
    super(parent);
  }

  /**
   * Retrieve the FieldWriter, promoting if it is not a FieldWriter of the specified type
   * @param type
   * @return
   */
  abstract protected FieldWriter getWriter(MinorType type);

  /**
   * Return the current FieldWriter
   * @return
   */
  abstract protected FieldWriter getWriter();

  @Override
  public void start() {
    getWriter(MinorType.MAP).start();
  }

  @Override
  public void end() {
    getWriter(MinorType.MAP).end();
  }

  @Override
  public void startList() {
    getWriter(MinorType.LIST).startList();
  }

  @Override
  public void endList() {
    getWriter(MinorType.LIST).endList();
  }

  @Override
  public void write(TinyIntHolder holder) {
    getWriter(MinorType.TINYINT).write(holder);
  }

  public void writeTinyInt(byte value) {
    getWriter(MinorType.TINYINT).writeTinyInt(value);
  }

  @Override
  public void write(UInt1Holder holder) {
    getWriter(MinorType.UINT1).write(holder);
  }

  public void writeUInt1(byte value) {
    getWriter(MinorType.UINT1).writeUInt1(value);
  }

  @Override
  public void write(UInt2Holder holder) {
    getWriter(MinorType.UINT2).write(holder);
  }

  public void writeUInt2(char value) {
    getWriter(MinorType.UINT2).writeUInt2(value);
  }

  @Override
  public void write(SmallIntHolder holder) {
    getWriter(MinorType.SMALLINT).write(holder);
  }

  public void writeSmallInt(short value) {
    getWriter(MinorType.SMALLINT).writeSmallInt(value);
  }

  @Override
  public void write(IntHolder holder) {
    getWriter(MinorType.INT).write(holder);
  }

  public void writeInt(int value) {
    getWriter(MinorType.INT).writeInt(value);
  }

  @Override
  public void write(UInt4Holder holder) {
    getWriter(MinorType.UINT4).write(holder);
  }

  public void writeUInt4(int value) {
    getWriter(MinorType.UINT4).writeUInt4(value);
  }

  @Override
  public void write(Float4Holder holder) {
    getWriter(MinorType.FLOAT4).write(holder);
  }

  public void writeFloat4(float value) {
    getWriter(MinorType.FLOAT4).writeFloat4(value);
  }

  @Override
  public void write(TimeHolder holder) {
    getWriter(MinorType.TIME).write(holder);
  }

  public void writeTime(int value) {
    getWriter(MinorType.TIME).writeTime(value);
  }

  @Override
  public void write(IntervalYearHolder holder) {
    getWriter(MinorType.INTERVALYEAR).write(holder);
  }

  public void writeIntervalYear(int value) {
    getWriter(MinorType.INTERVALYEAR).writeIntervalYear(value);
  }

  @Override
  public void write(BigIntHolder holder) {
    getWriter(MinorType.BIGINT).write(holder);
  }

  public void writeBigInt(long value) {
    getWriter(MinorType.BIGINT).writeBigInt(value);
  }

  @Override
  public void write(UInt8Holder holder) {
    getWriter(MinorType.UINT8).write(holder);
  }

  public void writeUInt8(long value) {
    getWriter(MinorType.UINT8).writeUInt8(value);
  }

  @Override
  public void write(Float8Holder holder) {
    getWriter(MinorType.FLOAT8).write(holder);
  }

  public void writeFloat8(double value) {
    getWriter(MinorType.FLOAT8).writeFloat8(value);
  }

  @Override
  public void write(DateHolder holder) {
    getWriter(MinorType.DATE).write(holder);
  }

  public void writeDate(long value) {
    getWriter(MinorType.DATE).writeDate(value);
  }

  @Override
  public void write(TimeStampHolder holder) {
    getWriter(MinorType.TIMESTAMP).write(holder);
  }

  public void writeTimeStamp(long value) {
    getWriter(MinorType.TIMESTAMP).writeTimeStamp(value);
  }

  @Override
  public void write(IntervalDayHolder holder) {
    getWriter(MinorType.INTERVALDAY).write(holder);
  }

  public void writeIntervalDay(int days, int milliseconds) {
    getWriter(MinorType.INTERVALDAY).writeIntervalDay(days, milliseconds);
  }

  @Override
  public void write(IntervalHolder holder) {
    getWriter(MinorType.INTERVAL).write(holder);
  }

  public void writeInterval(int months, int days, int milliseconds) {
    getWriter(MinorType.INTERVAL).writeInterval(months, days, milliseconds);
  }

  @Override
  public void write(VarBinaryHolder holder) {
    getWriter(MinorType.VARBINARY).write(holder);
  }

  public void writeVarBinary(int start, int end, ArrowBuf buffer) {
    getWriter(MinorType.VARBINARY).writeVarBinary(start, end, buffer);
  }

  @Override
  public void write(VarCharHolder holder) {
    getWriter(MinorType.VARCHAR).write(holder);
  }

  public void writeVarChar(int start, int end, ArrowBuf buffer) {
    getWriter(MinorType.VARCHAR).writeVarChar(start, end, buffer);
  }

  @Override
  public void write(Var16CharHolder holder) {
    getWriter(MinorType.VAR16CHAR).write(holder);
  }

  public void writeVar16Char(int start, int end, ArrowBuf buffer) {
    getWriter(MinorType.VAR16CHAR).writeVar16Char(start, end, buffer);
  }

  @Override
  public void write(BitHolder holder) {
    getWriter(MinorType.BIT).write(holder);
  }

  public void writeBit(int value) {
    getWriter(MinorType.BIT).writeBit(value);
  }


  public void writeNull() {
  }

  @Override
  public MapWriter map() {
    return getWriter(MinorType.LIST).map();
  }

  @Override
  public ListWriter list() {
    return getWriter(MinorType.LIST).list();
  }

  @Override
  public MapWriter map(String name) {
    return getWriter(MinorType.MAP).map(name);
  }

  @Override
  public ListWriter list(String name) {
    return getWriter(MinorType.MAP).list(name);
  }


  @Override
  public TinyIntWriter tinyInt(String name) {
    return getWriter(MinorType.MAP).tinyInt(name);
  }

  @Override
  public TinyIntWriter tinyInt() {
    return getWriter(MinorType.LIST).tinyInt();
  }


  @Override
  public UInt1Writer uInt1(String name) {
    return getWriter(MinorType.MAP).uInt1(name);
  }

  @Override
  public UInt1Writer uInt1() {
    return getWriter(MinorType.LIST).uInt1();
  }


  @Override
  public UInt2Writer uInt2(String name) {
    return getWriter(MinorType.MAP).uInt2(name);
  }

  @Override
  public UInt2Writer uInt2() {
    return getWriter(MinorType.LIST).uInt2();
  }


  @Override
  public SmallIntWriter smallInt(String name) {
    return getWriter(MinorType.MAP).smallInt(name);
  }

  @Override
  public SmallIntWriter smallInt() {
    return getWriter(MinorType.LIST).smallInt();
  }


  @Override
  public IntWriter integer(String name) {
    return getWriter(MinorType.MAP).integer(name);
  }

  @Override
  public IntWriter integer() {
    return getWriter(MinorType.LIST).integer();
  }


  @Override
  public UInt4Writer uInt4(String name) {
    return getWriter(MinorType.MAP).uInt4(name);
  }

  @Override
  public UInt4Writer uInt4() {
    return getWriter(MinorType.LIST).uInt4();
  }


  @Override
  public Float4Writer float4(String name) {
    return getWriter(MinorType.MAP).float4(name);
  }

  @Override
  public Float4Writer float4() {
    return getWriter(MinorType.LIST).float4();
  }


  @Override
  public TimeWriter time(String name) {
    return getWriter(MinorType.MAP).time(name);
  }

  @Override
  public TimeWriter time() {
    return getWriter(MinorType.LIST).time();
  }


  @Override
  public IntervalYearWriter intervalYear(String name) {
    return getWriter(MinorType.MAP).intervalYear(name);
  }

  @Override
  public IntervalYearWriter intervalYear() {
    return getWriter(MinorType.LIST).intervalYear();
  }


  @Override
  public BigIntWriter bigInt(String name) {
    return getWriter(MinorType.MAP).bigInt(name);
  }

  @Override
  public BigIntWriter bigInt() {
    return getWriter(MinorType.LIST).bigInt();
  }


  @Override
  public UInt8Writer uInt8(String name) {
    return getWriter(MinorType.MAP).uInt8(name);
  }

  @Override
  public UInt8Writer uInt8() {
    return getWriter(MinorType.LIST).uInt8();
  }


  @Override
  public Float8Writer float8(String name) {
    return getWriter(MinorType.MAP).float8(name);
  }

  @Override
  public Float8Writer float8() {
    return getWriter(MinorType.LIST).float8();
  }


  @Override
  public DateWriter date(String name) {
    return getWriter(MinorType.MAP).date(name);
  }

  @Override
  public DateWriter date() {
    return getWriter(MinorType.LIST).date();
  }


  @Override
  public TimeStampWriter timeStamp(String name) {
    return getWriter(MinorType.MAP).timeStamp(name);
  }

  @Override
  public TimeStampWriter timeStamp() {
    return getWriter(MinorType.LIST).timeStamp();
  }


  @Override
  public IntervalDayWriter intervalDay(String name) {
    return getWriter(MinorType.MAP).intervalDay(name);
  }

  @Override
  public IntervalDayWriter intervalDay() {
    return getWriter(MinorType.LIST).intervalDay();
  }


  @Override
  public IntervalWriter interval(String name) {
    return getWriter(MinorType.MAP).interval(name);
  }

  @Override
  public IntervalWriter interval() {
    return getWriter(MinorType.LIST).interval();
  }


  @Override
  public VarBinaryWriter varBinary(String name) {
    return getWriter(MinorType.MAP).varBinary(name);
  }

  @Override
  public VarBinaryWriter varBinary() {
    return getWriter(MinorType.LIST).varBinary();
  }


  @Override
  public VarCharWriter varChar(String name) {
    return getWriter(MinorType.MAP).varChar(name);
  }

  @Override
  public VarCharWriter varChar() {
    return getWriter(MinorType.LIST).varChar();
  }


  @Override
  public Var16CharWriter var16Char(String name) {
    return getWriter(MinorType.MAP).var16Char(name);
  }

  @Override
  public Var16CharWriter var16Char() {
    return getWriter(MinorType.LIST).var16Char();
  }


  @Override
  public BitWriter bit(String name) {
    return getWriter(MinorType.MAP).bit(name);
  }

  @Override
  public BitWriter bit() {
    return getWriter(MinorType.LIST).bit();
  }


  public void copyReader(FieldReader reader) {
    getWriter().copyReader(reader);
  }

  public void copyReaderToField(String name, FieldReader reader) {
    getWriter().copyReaderToField(name, reader);
  }
}
