

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
package org.apache.arrow.vector.complex.writer;


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
 * File generated from BaseWriter.java using FreeMarker.
 */
@SuppressWarnings("unused")
  public interface BaseWriter extends AutoCloseable, Positionable {
  FieldWriter getParent();
  int getValueCapacity();

  public interface MapWriter extends BaseWriter {

    MaterializedField getField();

    /**
     * Whether this writer is a map writer and is empty (has no children).
     * 
     * <p>
     *   Intended only for use in determining whether to add dummy vector to
     *   avoid empty (zero-column) schema, as in JsonReader.
     * </p>
     * 
     */
    boolean isEmptyMap();

    TinyIntWriter tinyInt(String name);
    UInt1Writer uInt1(String name);
    UInt2Writer uInt2(String name);
    SmallIntWriter smallInt(String name);
    IntWriter integer(String name);
    UInt4Writer uInt4(String name);
    Float4Writer float4(String name);
    TimeWriter time(String name);
    IntervalYearWriter intervalYear(String name);
    Decimal9Writer decimal9(String name, int scale, int precision);
    Decimal9Writer decimal9(String name);
    BigIntWriter bigInt(String name);
    UInt8Writer uInt8(String name);
    Float8Writer float8(String name);
    DateWriter date(String name);
    TimeStampWriter timeStamp(String name);
    Decimal18Writer decimal18(String name, int scale, int precision);
    Decimal18Writer decimal18(String name);
    IntervalDayWriter intervalDay(String name);
    IntervalWriter interval(String name);
    Decimal28DenseWriter decimal28Dense(String name, int scale, int precision);
    Decimal28DenseWriter decimal28Dense(String name);
    Decimal38DenseWriter decimal38Dense(String name, int scale, int precision);
    Decimal38DenseWriter decimal38Dense(String name);
    Decimal38SparseWriter decimal38Sparse(String name, int scale, int precision);
    Decimal38SparseWriter decimal38Sparse(String name);
    Decimal28SparseWriter decimal28Sparse(String name, int scale, int precision);
    Decimal28SparseWriter decimal28Sparse(String name);
    VarBinaryWriter varBinary(String name);
    VarCharWriter varChar(String name);
    Var16CharWriter var16Char(String name);
    BitWriter bit(String name);

    void copyReaderToField(String name, FieldReader reader);
    MapWriter map(String name);
    ListWriter list(String name);
    void start();
    void end();
  }

  public interface ListWriter extends BaseWriter {
    void startList();
    void endList();
    MapWriter map();
    ListWriter list();
    void copyReader(FieldReader reader);

    TinyIntWriter tinyInt();
    UInt1Writer uInt1();
    UInt2Writer uInt2();
    SmallIntWriter smallInt();
    IntWriter integer();
    UInt4Writer uInt4();
    Float4Writer float4();
    TimeWriter time();
    IntervalYearWriter intervalYear();
    Decimal9Writer decimal9();
    BigIntWriter bigInt();
    UInt8Writer uInt8();
    Float8Writer float8();
    DateWriter date();
    TimeStampWriter timeStamp();
    Decimal18Writer decimal18();
    IntervalDayWriter intervalDay();
    IntervalWriter interval();
    Decimal28DenseWriter decimal28Dense();
    Decimal38DenseWriter decimal38Dense();
    Decimal38SparseWriter decimal38Sparse();
    Decimal28SparseWriter decimal28Sparse();
    VarBinaryWriter varBinary();
    VarCharWriter varChar();
    Var16CharWriter var16Char();
    BitWriter bit();
  }

  public interface ScalarWriter extends
   TinyIntWriter,  UInt1Writer,  UInt2Writer,  SmallIntWriter,  IntWriter,  UInt4Writer,  Float4Writer,  TimeWriter,  IntervalYearWriter,  Decimal9Writer,  BigIntWriter,  UInt8Writer,  Float8Writer,  DateWriter,  TimeStampWriter,  Decimal18Writer,  IntervalDayWriter,  IntervalWriter,  Decimal28DenseWriter,  Decimal38DenseWriter,  Decimal38SparseWriter,  Decimal28SparseWriter,  VarBinaryWriter,  VarCharWriter,  Var16CharWriter,  BitWriter,  BaseWriter {}

  public interface ComplexWriter {
    void allocate();
    void clear();
    void copyReader(FieldReader reader);
    MapWriter rootAsMap();
    ListWriter rootAsList();

    void setPosition(int index);
    void setValueCount(int count);
    void reset();
  }

  public interface MapOrListWriter {
    void start();
    void end();
    MapOrListWriter map(String name);
    MapOrListWriter listoftmap(String name);
    MapOrListWriter list(String name);
    boolean isMapWriter();
    boolean isListWriter();
    VarCharWriter varChar(String name);
    IntWriter integer(String name);
    BigIntWriter bigInt(String name);
    Float4Writer float4(String name);
    Float8Writer float8(String name);
    BitWriter bit(String name);
    VarBinaryWriter binary(String name);
  }
}
