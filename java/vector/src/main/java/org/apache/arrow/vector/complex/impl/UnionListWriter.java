

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
 * This class is generated using freemarker and the UnionListWriter.java template.
 */

@SuppressWarnings("unused")
public class UnionListWriter extends AbstractFieldWriter {

  private ListVector vector;
  private UInt4Vector offsets;
  private PromotableWriter writer;
  private boolean inMap = false;
  private String mapName;
  private int lastIndex = 0;

  public UnionListWriter(ListVector vector) {
    super(null);
    this.vector = vector;
    this.writer = new PromotableWriter(vector.getDataVector(), vector);
    this.offsets = vector.getOffsetVector();
  }

  public UnionListWriter(ListVector vector, AbstractFieldWriter parent) {
    this(vector);
  }

  @Override
  public void allocate() {
    vector.allocateNew();
  }

  @Override
  public void clear() {
    vector.clear();
  }

  @Override
  public MaterializedField getField() {
    return null;
  }

  @Override
  public int getValueCapacity() {
    return vector.getValueCapacity();
  }

  @Override
  public void close() throws Exception {

  }



  @Override
  public TinyIntWriter tinyInt() {
    return this;
  }

  @Override
  public TinyIntWriter tinyInt(String name) {
    assert inMap;
    mapName = name;
    final int nextOffset = offsets.getAccessor().get(idx() + 1);
    vector.getMutator().setNotNull(idx());
    writer.setPosition(nextOffset);
    TinyIntWriter tinyIntWriter = writer.tinyInt(name);
    return tinyIntWriter;
  }




  @Override
  public UInt1Writer uInt1() {
    return this;
  }

  @Override
  public UInt1Writer uInt1(String name) {
    assert inMap;
    mapName = name;
    final int nextOffset = offsets.getAccessor().get(idx() + 1);
    vector.getMutator().setNotNull(idx());
    writer.setPosition(nextOffset);
    UInt1Writer uInt1Writer = writer.uInt1(name);
    return uInt1Writer;
  }




  @Override
  public UInt2Writer uInt2() {
    return this;
  }

  @Override
  public UInt2Writer uInt2(String name) {
    assert inMap;
    mapName = name;
    final int nextOffset = offsets.getAccessor().get(idx() + 1);
    vector.getMutator().setNotNull(idx());
    writer.setPosition(nextOffset);
    UInt2Writer uInt2Writer = writer.uInt2(name);
    return uInt2Writer;
  }




  @Override
  public SmallIntWriter smallInt() {
    return this;
  }

  @Override
  public SmallIntWriter smallInt(String name) {
    assert inMap;
    mapName = name;
    final int nextOffset = offsets.getAccessor().get(idx() + 1);
    vector.getMutator().setNotNull(idx());
    writer.setPosition(nextOffset);
    SmallIntWriter smallIntWriter = writer.smallInt(name);
    return smallIntWriter;
  }




  @Override
  public IntWriter integer() {
    return this;
  }

  @Override
  public IntWriter integer(String name) {
    assert inMap;
    mapName = name;
    final int nextOffset = offsets.getAccessor().get(idx() + 1);
    vector.getMutator().setNotNull(idx());
    writer.setPosition(nextOffset);
    IntWriter intWriter = writer.integer(name);
    return intWriter;
  }




  @Override
  public UInt4Writer uInt4() {
    return this;
  }

  @Override
  public UInt4Writer uInt4(String name) {
    assert inMap;
    mapName = name;
    final int nextOffset = offsets.getAccessor().get(idx() + 1);
    vector.getMutator().setNotNull(idx());
    writer.setPosition(nextOffset);
    UInt4Writer uInt4Writer = writer.uInt4(name);
    return uInt4Writer;
  }




  @Override
  public Float4Writer float4() {
    return this;
  }

  @Override
  public Float4Writer float4(String name) {
    assert inMap;
    mapName = name;
    final int nextOffset = offsets.getAccessor().get(idx() + 1);
    vector.getMutator().setNotNull(idx());
    writer.setPosition(nextOffset);
    Float4Writer float4Writer = writer.float4(name);
    return float4Writer;
  }




  @Override
  public TimeWriter time() {
    return this;
  }

  @Override
  public TimeWriter time(String name) {
    assert inMap;
    mapName = name;
    final int nextOffset = offsets.getAccessor().get(idx() + 1);
    vector.getMutator().setNotNull(idx());
    writer.setPosition(nextOffset);
    TimeWriter timeWriter = writer.time(name);
    return timeWriter;
  }




  @Override
  public IntervalYearWriter intervalYear() {
    return this;
  }

  @Override
  public IntervalYearWriter intervalYear(String name) {
    assert inMap;
    mapName = name;
    final int nextOffset = offsets.getAccessor().get(idx() + 1);
    vector.getMutator().setNotNull(idx());
    writer.setPosition(nextOffset);
    IntervalYearWriter intervalYearWriter = writer.intervalYear(name);
    return intervalYearWriter;
  }






  @Override
  public BigIntWriter bigInt() {
    return this;
  }

  @Override
  public BigIntWriter bigInt(String name) {
    assert inMap;
    mapName = name;
    final int nextOffset = offsets.getAccessor().get(idx() + 1);
    vector.getMutator().setNotNull(idx());
    writer.setPosition(nextOffset);
    BigIntWriter bigIntWriter = writer.bigInt(name);
    return bigIntWriter;
  }




  @Override
  public UInt8Writer uInt8() {
    return this;
  }

  @Override
  public UInt8Writer uInt8(String name) {
    assert inMap;
    mapName = name;
    final int nextOffset = offsets.getAccessor().get(idx() + 1);
    vector.getMutator().setNotNull(idx());
    writer.setPosition(nextOffset);
    UInt8Writer uInt8Writer = writer.uInt8(name);
    return uInt8Writer;
  }




  @Override
  public Float8Writer float8() {
    return this;
  }

  @Override
  public Float8Writer float8(String name) {
    assert inMap;
    mapName = name;
    final int nextOffset = offsets.getAccessor().get(idx() + 1);
    vector.getMutator().setNotNull(idx());
    writer.setPosition(nextOffset);
    Float8Writer float8Writer = writer.float8(name);
    return float8Writer;
  }




  @Override
  public DateWriter date() {
    return this;
  }

  @Override
  public DateWriter date(String name) {
    assert inMap;
    mapName = name;
    final int nextOffset = offsets.getAccessor().get(idx() + 1);
    vector.getMutator().setNotNull(idx());
    writer.setPosition(nextOffset);
    DateWriter dateWriter = writer.date(name);
    return dateWriter;
  }




  @Override
  public TimeStampWriter timeStamp() {
    return this;
  }

  @Override
  public TimeStampWriter timeStamp(String name) {
    assert inMap;
    mapName = name;
    final int nextOffset = offsets.getAccessor().get(idx() + 1);
    vector.getMutator().setNotNull(idx());
    writer.setPosition(nextOffset);
    TimeStampWriter timeStampWriter = writer.timeStamp(name);
    return timeStampWriter;
  }






  @Override
  public IntervalDayWriter intervalDay() {
    return this;
  }

  @Override
  public IntervalDayWriter intervalDay(String name) {
    assert inMap;
    mapName = name;
    final int nextOffset = offsets.getAccessor().get(idx() + 1);
    vector.getMutator().setNotNull(idx());
    writer.setPosition(nextOffset);
    IntervalDayWriter intervalDayWriter = writer.intervalDay(name);
    return intervalDayWriter;
  }




  @Override
  public IntervalWriter interval() {
    return this;
  }

  @Override
  public IntervalWriter interval(String name) {
    assert inMap;
    mapName = name;
    final int nextOffset = offsets.getAccessor().get(idx() + 1);
    vector.getMutator().setNotNull(idx());
    writer.setPosition(nextOffset);
    IntervalWriter intervalWriter = writer.interval(name);
    return intervalWriter;
  }












  @Override
  public VarBinaryWriter varBinary() {
    return this;
  }

  @Override
  public VarBinaryWriter varBinary(String name) {
    assert inMap;
    mapName = name;
    final int nextOffset = offsets.getAccessor().get(idx() + 1);
    vector.getMutator().setNotNull(idx());
    writer.setPosition(nextOffset);
    VarBinaryWriter varBinaryWriter = writer.varBinary(name);
    return varBinaryWriter;
  }




  @Override
  public VarCharWriter varChar() {
    return this;
  }

  @Override
  public VarCharWriter varChar(String name) {
    assert inMap;
    mapName = name;
    final int nextOffset = offsets.getAccessor().get(idx() + 1);
    vector.getMutator().setNotNull(idx());
    writer.setPosition(nextOffset);
    VarCharWriter varCharWriter = writer.varChar(name);
    return varCharWriter;
  }




  @Override
  public Var16CharWriter var16Char() {
    return this;
  }

  @Override
  public Var16CharWriter var16Char(String name) {
    assert inMap;
    mapName = name;
    final int nextOffset = offsets.getAccessor().get(idx() + 1);
    vector.getMutator().setNotNull(idx());
    writer.setPosition(nextOffset);
    Var16CharWriter var16CharWriter = writer.var16Char(name);
    return var16CharWriter;
  }




  @Override
  public BitWriter bit() {
    return this;
  }

  @Override
  public BitWriter bit(String name) {
    assert inMap;
    mapName = name;
    final int nextOffset = offsets.getAccessor().get(idx() + 1);
    vector.getMutator().setNotNull(idx());
    writer.setPosition(nextOffset);
    BitWriter bitWriter = writer.bit(name);
    return bitWriter;
  }



  @Override
  public MapWriter map() {
    inMap = true;
    return this;
  }

  @Override
  public ListWriter list() {
    final int nextOffset = offsets.getAccessor().get(idx() + 1);
    vector.getMutator().setNotNull(idx());
    offsets.getMutator().setSafe(idx() + 1, nextOffset + 1);
    writer.setPosition(nextOffset);
    return writer;
  }

  @Override
  public ListWriter list(String name) {
    final int nextOffset = offsets.getAccessor().get(idx() + 1);
    vector.getMutator().setNotNull(idx());
    writer.setPosition(nextOffset);
    ListWriter listWriter = writer.list(name);
    return listWriter;
  }

  @Override
  public MapWriter map(String name) {
    MapWriter mapWriter = writer.map(name);
    return mapWriter;
  }

  @Override
  public void startList() {
    vector.getMutator().startNewValue(idx());
  }

  @Override
  public void endList() {

  }

  @Override
  public void start() {
    assert inMap;
    final int nextOffset = offsets.getAccessor().get(idx() + 1);
    vector.getMutator().setNotNull(idx());
    offsets.getMutator().setSafe(idx() + 1, nextOffset);
    writer.setPosition(nextOffset);
  }

  @Override
  public void end() {
    if (inMap) {
      inMap = false;
      final int nextOffset = offsets.getAccessor().get(idx() + 1);
      offsets.getMutator().setSafe(idx() + 1, nextOffset + 1);
    }
  }



  @Override
  public void writeTinyInt(byte value) {
    assert !inMap;
    final int nextOffset = offsets.getAccessor().get(idx() + 1);
    vector.getMutator().setNotNull(idx());
    writer.setPosition(nextOffset);
    writer.writeTinyInt(value);
    offsets.getMutator().setSafe(idx() + 1, nextOffset + 1);
  }




  @Override
  public void writeUInt1(byte value) {
    assert !inMap;
    final int nextOffset = offsets.getAccessor().get(idx() + 1);
    vector.getMutator().setNotNull(idx());
    writer.setPosition(nextOffset);
    writer.writeUInt1(value);
    offsets.getMutator().setSafe(idx() + 1, nextOffset + 1);
  }




  @Override
  public void writeUInt2(char value) {
    assert !inMap;
    final int nextOffset = offsets.getAccessor().get(idx() + 1);
    vector.getMutator().setNotNull(idx());
    writer.setPosition(nextOffset);
    writer.writeUInt2(value);
    offsets.getMutator().setSafe(idx() + 1, nextOffset + 1);
  }




  @Override
  public void writeSmallInt(short value) {
    assert !inMap;
    final int nextOffset = offsets.getAccessor().get(idx() + 1);
    vector.getMutator().setNotNull(idx());
    writer.setPosition(nextOffset);
    writer.writeSmallInt(value);
    offsets.getMutator().setSafe(idx() + 1, nextOffset + 1);
  }




  @Override
  public void writeInt(int value) {
    assert !inMap;
    final int nextOffset = offsets.getAccessor().get(idx() + 1);
    vector.getMutator().setNotNull(idx());
    writer.setPosition(nextOffset);
    writer.writeInt(value);
    offsets.getMutator().setSafe(idx() + 1, nextOffset + 1);
  }




  @Override
  public void writeUInt4(int value) {
    assert !inMap;
    final int nextOffset = offsets.getAccessor().get(idx() + 1);
    vector.getMutator().setNotNull(idx());
    writer.setPosition(nextOffset);
    writer.writeUInt4(value);
    offsets.getMutator().setSafe(idx() + 1, nextOffset + 1);
  }




  @Override
  public void writeFloat4(float value) {
    assert !inMap;
    final int nextOffset = offsets.getAccessor().get(idx() + 1);
    vector.getMutator().setNotNull(idx());
    writer.setPosition(nextOffset);
    writer.writeFloat4(value);
    offsets.getMutator().setSafe(idx() + 1, nextOffset + 1);
  }




  @Override
  public void writeTime(int value) {
    assert !inMap;
    final int nextOffset = offsets.getAccessor().get(idx() + 1);
    vector.getMutator().setNotNull(idx());
    writer.setPosition(nextOffset);
    writer.writeTime(value);
    offsets.getMutator().setSafe(idx() + 1, nextOffset + 1);
  }




  @Override
  public void writeIntervalYear(int value) {
    assert !inMap;
    final int nextOffset = offsets.getAccessor().get(idx() + 1);
    vector.getMutator().setNotNull(idx());
    writer.setPosition(nextOffset);
    writer.writeIntervalYear(value);
    offsets.getMutator().setSafe(idx() + 1, nextOffset + 1);
  }






  @Override
  public void writeBigInt(long value) {
    assert !inMap;
    final int nextOffset = offsets.getAccessor().get(idx() + 1);
    vector.getMutator().setNotNull(idx());
    writer.setPosition(nextOffset);
    writer.writeBigInt(value);
    offsets.getMutator().setSafe(idx() + 1, nextOffset + 1);
  }




  @Override
  public void writeUInt8(long value) {
    assert !inMap;
    final int nextOffset = offsets.getAccessor().get(idx() + 1);
    vector.getMutator().setNotNull(idx());
    writer.setPosition(nextOffset);
    writer.writeUInt8(value);
    offsets.getMutator().setSafe(idx() + 1, nextOffset + 1);
  }




  @Override
  public void writeFloat8(double value) {
    assert !inMap;
    final int nextOffset = offsets.getAccessor().get(idx() + 1);
    vector.getMutator().setNotNull(idx());
    writer.setPosition(nextOffset);
    writer.writeFloat8(value);
    offsets.getMutator().setSafe(idx() + 1, nextOffset + 1);
  }




  @Override
  public void writeDate(long value) {
    assert !inMap;
    final int nextOffset = offsets.getAccessor().get(idx() + 1);
    vector.getMutator().setNotNull(idx());
    writer.setPosition(nextOffset);
    writer.writeDate(value);
    offsets.getMutator().setSafe(idx() + 1, nextOffset + 1);
  }




  @Override
  public void writeTimeStamp(long value) {
    assert !inMap;
    final int nextOffset = offsets.getAccessor().get(idx() + 1);
    vector.getMutator().setNotNull(idx());
    writer.setPosition(nextOffset);
    writer.writeTimeStamp(value);
    offsets.getMutator().setSafe(idx() + 1, nextOffset + 1);
  }






  @Override
  public void writeIntervalDay(int days, int milliseconds) {
    assert !inMap;
    final int nextOffset = offsets.getAccessor().get(idx() + 1);
    vector.getMutator().setNotNull(idx());
    writer.setPosition(nextOffset);
    writer.writeIntervalDay(days, milliseconds);
    offsets.getMutator().setSafe(idx() + 1, nextOffset + 1);
  }




  @Override
  public void writeInterval(int months, int days, int milliseconds) {
    assert !inMap;
    final int nextOffset = offsets.getAccessor().get(idx() + 1);
    vector.getMutator().setNotNull(idx());
    writer.setPosition(nextOffset);
    writer.writeInterval(months, days, milliseconds);
    offsets.getMutator().setSafe(idx() + 1, nextOffset + 1);
  }












  @Override
  public void writeVarBinary(int start, int end, ArrowBuf buffer) {
    assert !inMap;
    final int nextOffset = offsets.getAccessor().get(idx() + 1);
    vector.getMutator().setNotNull(idx());
    writer.setPosition(nextOffset);
    writer.writeVarBinary(start, end, buffer);
    offsets.getMutator().setSafe(idx() + 1, nextOffset + 1);
  }




  @Override
  public void writeVarChar(int start, int end, ArrowBuf buffer) {
    assert !inMap;
    final int nextOffset = offsets.getAccessor().get(idx() + 1);
    vector.getMutator().setNotNull(idx());
    writer.setPosition(nextOffset);
    writer.writeVarChar(start, end, buffer);
    offsets.getMutator().setSafe(idx() + 1, nextOffset + 1);
  }




  @Override
  public void writeVar16Char(int start, int end, ArrowBuf buffer) {
    assert !inMap;
    final int nextOffset = offsets.getAccessor().get(idx() + 1);
    vector.getMutator().setNotNull(idx());
    writer.setPosition(nextOffset);
    writer.writeVar16Char(start, end, buffer);
    offsets.getMutator().setSafe(idx() + 1, nextOffset + 1);
  }




  @Override
  public void writeBit(int value) {
    assert !inMap;
    final int nextOffset = offsets.getAccessor().get(idx() + 1);
    vector.getMutator().setNotNull(idx());
    writer.setPosition(nextOffset);
    writer.writeBit(value);
    offsets.getMutator().setSafe(idx() + 1, nextOffset + 1);
  }



}
