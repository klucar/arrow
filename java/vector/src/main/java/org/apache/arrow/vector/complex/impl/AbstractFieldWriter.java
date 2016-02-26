

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
 * This class is generated using freemarker and the AbstractFieldWriter.java template.
 */
@SuppressWarnings("unused")
abstract class AbstractFieldWriter extends AbstractBaseWriter implements FieldWriter {
  AbstractFieldWriter(FieldWriter parent) {
    super(parent);
  }

  @Override
  public void start() {
    throw new IllegalStateException(String.format("You tried to start when you are using a ValueWriter of type %s.", this.getClass().getSimpleName()));
  }

  @Override
  public void end() {
    throw new IllegalStateException(String.format("You tried to end when you are using a ValueWriter of type %s.", this.getClass().getSimpleName()));
  }

  @Override
  public void startList() {
    throw new IllegalStateException(String.format("You tried to start when you are using a ValueWriter of type %s.", this.getClass().getSimpleName()));
  }

  @Override
  public void endList() {
    throw new IllegalStateException(String.format("You tried to end when you are using a ValueWriter of type %s.", this.getClass().getSimpleName()));
  }

  @Override
  public void write(TinyIntHolder holder) {
    fail("TinyInt");
  }

  public void writeTinyInt(byte value) {
    fail("TinyInt");
  }

  @Override
  public void write(UInt1Holder holder) {
    fail("UInt1");
  }

  public void writeUInt1(byte value) {
    fail("UInt1");
  }

  @Override
  public void write(UInt2Holder holder) {
    fail("UInt2");
  }

  public void writeUInt2(char value) {
    fail("UInt2");
  }

  @Override
  public void write(SmallIntHolder holder) {
    fail("SmallInt");
  }

  public void writeSmallInt(short value) {
    fail("SmallInt");
  }

  @Override
  public void write(IntHolder holder) {
    fail("Int");
  }

  public void writeInt(int value) {
    fail("Int");
  }

  @Override
  public void write(UInt4Holder holder) {
    fail("UInt4");
  }

  public void writeUInt4(int value) {
    fail("UInt4");
  }

  @Override
  public void write(Float4Holder holder) {
    fail("Float4");
  }

  public void writeFloat4(float value) {
    fail("Float4");
  }

  @Override
  public void write(TimeHolder holder) {
    fail("Time");
  }

  public void writeTime(int value) {
    fail("Time");
  }

  @Override
  public void write(IntervalYearHolder holder) {
    fail("IntervalYear");
  }

  public void writeIntervalYear(int value) {
    fail("IntervalYear");
  }

  @Override
  public void write(Decimal9Holder holder) {
    fail("Decimal9");
  }

  public void writeDecimal9(int value, int scale, int precision) {
    fail("Decimal9");
  }

  @Override
  public void write(BigIntHolder holder) {
    fail("BigInt");
  }

  public void writeBigInt(long value) {
    fail("BigInt");
  }

  @Override
  public void write(UInt8Holder holder) {
    fail("UInt8");
  }

  public void writeUInt8(long value) {
    fail("UInt8");
  }

  @Override
  public void write(Float8Holder holder) {
    fail("Float8");
  }

  public void writeFloat8(double value) {
    fail("Float8");
  }

  @Override
  public void write(DateHolder holder) {
    fail("Date");
  }

  public void writeDate(long value) {
    fail("Date");
  }

  @Override
  public void write(TimeStampHolder holder) {
    fail("TimeStamp");
  }

  public void writeTimeStamp(long value) {
    fail("TimeStamp");
  }

  @Override
  public void write(Decimal18Holder holder) {
    fail("Decimal18");
  }

  public void writeDecimal18(long value, int scale, int precision) {
    fail("Decimal18");
  }

  @Override
  public void write(IntervalDayHolder holder) {
    fail("IntervalDay");
  }

  public void writeIntervalDay(int days, int milliseconds) {
    fail("IntervalDay");
  }

  @Override
  public void write(IntervalHolder holder) {
    fail("Interval");
  }

  public void writeInterval(int months, int days, int milliseconds) {
    fail("Interval");
  }

  @Override
  public void write(Decimal28DenseHolder holder) {
    fail("Decimal28Dense");
  }

  public void writeDecimal28Dense(int start, ArrowBuf buffer, int scale, int precision) {
    fail("Decimal28Dense");
  }

  @Override
  public void write(Decimal38DenseHolder holder) {
    fail("Decimal38Dense");
  }

  public void writeDecimal38Dense(int start, ArrowBuf buffer, int scale, int precision) {
    fail("Decimal38Dense");
  }

  @Override
  public void write(Decimal38SparseHolder holder) {
    fail("Decimal38Sparse");
  }

  public void writeDecimal38Sparse(int start, ArrowBuf buffer, int scale, int precision) {
    fail("Decimal38Sparse");
  }

  @Override
  public void write(Decimal28SparseHolder holder) {
    fail("Decimal28Sparse");
  }

  public void writeDecimal28Sparse(int start, ArrowBuf buffer, int scale, int precision) {
    fail("Decimal28Sparse");
  }

  @Override
  public void write(VarBinaryHolder holder) {
    fail("VarBinary");
  }

  public void writeVarBinary(int start, int end, ArrowBuf buffer) {
    fail("VarBinary");
  }

  @Override
  public void write(VarCharHolder holder) {
    fail("VarChar");
  }

  public void writeVarChar(int start, int end, ArrowBuf buffer) {
    fail("VarChar");
  }

  @Override
  public void write(Var16CharHolder holder) {
    fail("Var16Char");
  }

  public void writeVar16Char(int start, int end, ArrowBuf buffer) {
    fail("Var16Char");
  }

  @Override
  public void write(BitHolder holder) {
    fail("Bit");
  }

  public void writeBit(int value) {
    fail("Bit");
  }


  public void writeNull() {
    fail("Bit");
  }

  /**
   * This implementation returns {@code false}.
   * <p>  
   *   Must be overridden by map writers.
   * </p>  
   */
  @Override
  public boolean isEmptyMap() {
    return false;
  }

  @Override
  public MapWriter map() {
    fail("Map");
    return null;
  }

  @Override
  public ListWriter list() {
    fail("List");
    return null;
  }

  @Override
  public MapWriter map(String name) {
    fail("Map");
    return null;
  }

  @Override
  public ListWriter list(String name) {
    fail("List");
    return null;
  }


  @Override
  public TinyIntWriter tinyInt(String name) {
    fail("TinyInt");
    return null;
  }

  @Override
  public TinyIntWriter tinyInt() {
    fail("TinyInt");
    return null;
  }


  @Override
  public UInt1Writer uInt1(String name) {
    fail("UInt1");
    return null;
  }

  @Override
  public UInt1Writer uInt1() {
    fail("UInt1");
    return null;
  }


  @Override
  public UInt2Writer uInt2(String name) {
    fail("UInt2");
    return null;
  }

  @Override
  public UInt2Writer uInt2() {
    fail("UInt2");
    return null;
  }


  @Override
  public SmallIntWriter smallInt(String name) {
    fail("SmallInt");
    return null;
  }

  @Override
  public SmallIntWriter smallInt() {
    fail("SmallInt");
    return null;
  }


  @Override
  public IntWriter integer(String name) {
    fail("Int");
    return null;
  }

  @Override
  public IntWriter integer() {
    fail("Int");
    return null;
  }


  @Override
  public UInt4Writer uInt4(String name) {
    fail("UInt4");
    return null;
  }

  @Override
  public UInt4Writer uInt4() {
    fail("UInt4");
    return null;
  }


  @Override
  public Float4Writer float4(String name) {
    fail("Float4");
    return null;
  }

  @Override
  public Float4Writer float4() {
    fail("Float4");
    return null;
  }


  @Override
  public TimeWriter time(String name) {
    fail("Time");
    return null;
  }

  @Override
  public TimeWriter time() {
    fail("Time");
    return null;
  }


  @Override
  public IntervalYearWriter intervalYear(String name) {
    fail("IntervalYear");
    return null;
  }

  @Override
  public IntervalYearWriter intervalYear() {
    fail("IntervalYear");
    return null;
  }

  public Decimal9Writer decimal9(String name, int scale, int precision) {
    fail("Decimal9");
    return null;
  }

  @Override
  public Decimal9Writer decimal9(String name) {
    fail("Decimal9");
    return null;
  }

  @Override
  public Decimal9Writer decimal9() {
    fail("Decimal9");
    return null;
  }


  @Override
  public BigIntWriter bigInt(String name) {
    fail("BigInt");
    return null;
  }

  @Override
  public BigIntWriter bigInt() {
    fail("BigInt");
    return null;
  }


  @Override
  public UInt8Writer uInt8(String name) {
    fail("UInt8");
    return null;
  }

  @Override
  public UInt8Writer uInt8() {
    fail("UInt8");
    return null;
  }


  @Override
  public Float8Writer float8(String name) {
    fail("Float8");
    return null;
  }

  @Override
  public Float8Writer float8() {
    fail("Float8");
    return null;
  }


  @Override
  public DateWriter date(String name) {
    fail("Date");
    return null;
  }

  @Override
  public DateWriter date() {
    fail("Date");
    return null;
  }


  @Override
  public TimeStampWriter timeStamp(String name) {
    fail("TimeStamp");
    return null;
  }

  @Override
  public TimeStampWriter timeStamp() {
    fail("TimeStamp");
    return null;
  }

  public Decimal18Writer decimal18(String name, int scale, int precision) {
    fail("Decimal18");
    return null;
  }

  @Override
  public Decimal18Writer decimal18(String name) {
    fail("Decimal18");
    return null;
  }

  @Override
  public Decimal18Writer decimal18() {
    fail("Decimal18");
    return null;
  }


  @Override
  public IntervalDayWriter intervalDay(String name) {
    fail("IntervalDay");
    return null;
  }

  @Override
  public IntervalDayWriter intervalDay() {
    fail("IntervalDay");
    return null;
  }


  @Override
  public IntervalWriter interval(String name) {
    fail("Interval");
    return null;
  }

  @Override
  public IntervalWriter interval() {
    fail("Interval");
    return null;
  }

  public Decimal28DenseWriter decimal28Dense(String name, int scale, int precision) {
    fail("Decimal28Dense");
    return null;
  }

  @Override
  public Decimal28DenseWriter decimal28Dense(String name) {
    fail("Decimal28Dense");
    return null;
  }

  @Override
  public Decimal28DenseWriter decimal28Dense() {
    fail("Decimal28Dense");
    return null;
  }

  public Decimal38DenseWriter decimal38Dense(String name, int scale, int precision) {
    fail("Decimal38Dense");
    return null;
  }

  @Override
  public Decimal38DenseWriter decimal38Dense(String name) {
    fail("Decimal38Dense");
    return null;
  }

  @Override
  public Decimal38DenseWriter decimal38Dense() {
    fail("Decimal38Dense");
    return null;
  }

  public Decimal38SparseWriter decimal38Sparse(String name, int scale, int precision) {
    fail("Decimal38Sparse");
    return null;
  }

  @Override
  public Decimal38SparseWriter decimal38Sparse(String name) {
    fail("Decimal38Sparse");
    return null;
  }

  @Override
  public Decimal38SparseWriter decimal38Sparse() {
    fail("Decimal38Sparse");
    return null;
  }

  public Decimal28SparseWriter decimal28Sparse(String name, int scale, int precision) {
    fail("Decimal28Sparse");
    return null;
  }

  @Override
  public Decimal28SparseWriter decimal28Sparse(String name) {
    fail("Decimal28Sparse");
    return null;
  }

  @Override
  public Decimal28SparseWriter decimal28Sparse() {
    fail("Decimal28Sparse");
    return null;
  }


  @Override
  public VarBinaryWriter varBinary(String name) {
    fail("VarBinary");
    return null;
  }

  @Override
  public VarBinaryWriter varBinary() {
    fail("VarBinary");
    return null;
  }


  @Override
  public VarCharWriter varChar(String name) {
    fail("VarChar");
    return null;
  }

  @Override
  public VarCharWriter varChar() {
    fail("VarChar");
    return null;
  }


  @Override
  public Var16CharWriter var16Char(String name) {
    fail("Var16Char");
    return null;
  }

  @Override
  public Var16CharWriter var16Char() {
    fail("Var16Char");
    return null;
  }


  @Override
  public BitWriter bit(String name) {
    fail("Bit");
    return null;
  }

  @Override
  public BitWriter bit() {
    fail("Bit");
    return null;
  }


  public void copyReader(FieldReader reader) {
    fail("Copy FieldReader");
  }

  public void copyReaderToField(String name, FieldReader reader) {
    fail("Copy FieldReader to STring");
  }

  private void fail(String name) {
    throw new IllegalArgumentException(String.format("You tried to write a %s type when you are using a ValueWriter of type %s.", name, this.getClass().getSimpleName()));
  }
}
