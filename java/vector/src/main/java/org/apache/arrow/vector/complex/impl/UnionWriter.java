

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
 * This class is generated using freemarker and the UnionWriter.java template.
 */
@SuppressWarnings("unused")
public class UnionWriter extends AbstractFieldWriter implements FieldWriter {

  UnionVector data;
  private MapWriter mapWriter;
  private UnionListWriter listWriter;
  private List<BaseWriter> writers = Lists.newArrayList();

  public UnionWriter(BufferAllocator allocator) {
    super(null);
  }

  public UnionWriter(UnionVector vector) {
    super(null);
    data = vector;
  }

  public UnionWriter(UnionVector vector, FieldWriter parent) {
    super(null);
    data = vector;
  }

  @Override
  public void setPosition(int index) {
    super.setPosition(index);
    for (BaseWriter writer : writers) {
      writer.setPosition(index);
    }
  }


  @Override
  public void start() {
    data.getMutator().setType(idx(), MinorType.MAP);
    getMapWriter().start();
  }

  @Override
  public void end() {
    getMapWriter().end();
  }

  @Override
  public void startList() {
    getListWriter().startList();
    data.getMutator().setType(idx(), MinorType.LIST);
  }

  @Override
  public void endList() {
    getListWriter().endList();
  }

  private MapWriter getMapWriter() {
    if (mapWriter == null) {
      mapWriter = new SingleMapWriter(data.getMap(), null, true);
      mapWriter.setPosition(idx());
      writers.add(mapWriter);
    }
    return mapWriter;
  }

  public MapWriter asMap() {
    data.getMutator().setType(idx(), MinorType.MAP);
    return getMapWriter();
  }

  private ListWriter getListWriter() {
    if (listWriter == null) {
      listWriter = new UnionListWriter(data.getList());
      listWriter.setPosition(idx());
      writers.add(listWriter);
    }
    return listWriter;
  }

  public ListWriter asList() {
    data.getMutator().setType(idx(), MinorType.LIST);
    return getListWriter();
  }



  private TinyIntWriter tinyIntWriter;

  private TinyIntWriter getTinyIntWriter() {
    if (tinyIntWriter == null) {
      tinyIntWriter = new NullableTinyIntWriterImpl(data.getTinyIntVector(), null);
      tinyIntWriter.setPosition(idx());
      writers.add(tinyIntWriter);
    }
    return tinyIntWriter;
  }

  public TinyIntWriter asTinyInt() {
    data.getMutator().setType(idx(), MinorType.TINYINT);
    return getTinyIntWriter();
  }

  @Override
  public void write(TinyIntHolder holder) {
    data.getMutator().setType(idx(), MinorType.TINYINT);
    getTinyIntWriter().setPosition(idx());
    getTinyIntWriter().writeTinyInt(holder.value);
  }

  public void writeTinyInt(byte value) {
    data.getMutator().setType(idx(), MinorType.TINYINT);
    getTinyIntWriter().setPosition(idx());
    getTinyIntWriter().writeTinyInt(value);
  }



  private UInt1Writer uInt1Writer;

  private UInt1Writer getUInt1Writer() {
    if (uInt1Writer == null) {
      uInt1Writer = new NullableUInt1WriterImpl(data.getUInt1Vector(), null);
      uInt1Writer.setPosition(idx());
      writers.add(uInt1Writer);
    }
    return uInt1Writer;
  }

  public UInt1Writer asUInt1() {
    data.getMutator().setType(idx(), MinorType.UINT1);
    return getUInt1Writer();
  }

  @Override
  public void write(UInt1Holder holder) {
    data.getMutator().setType(idx(), MinorType.UINT1);
    getUInt1Writer().setPosition(idx());
    getUInt1Writer().writeUInt1(holder.value);
  }

  public void writeUInt1(byte value) {
    data.getMutator().setType(idx(), MinorType.UINT1);
    getUInt1Writer().setPosition(idx());
    getUInt1Writer().writeUInt1(value);
  }



  private UInt2Writer uInt2Writer;

  private UInt2Writer getUInt2Writer() {
    if (uInt2Writer == null) {
      uInt2Writer = new NullableUInt2WriterImpl(data.getUInt2Vector(), null);
      uInt2Writer.setPosition(idx());
      writers.add(uInt2Writer);
    }
    return uInt2Writer;
  }

  public UInt2Writer asUInt2() {
    data.getMutator().setType(idx(), MinorType.UINT2);
    return getUInt2Writer();
  }

  @Override
  public void write(UInt2Holder holder) {
    data.getMutator().setType(idx(), MinorType.UINT2);
    getUInt2Writer().setPosition(idx());
    getUInt2Writer().writeUInt2(holder.value);
  }

  public void writeUInt2(char value) {
    data.getMutator().setType(idx(), MinorType.UINT2);
    getUInt2Writer().setPosition(idx());
    getUInt2Writer().writeUInt2(value);
  }



  private SmallIntWriter smallIntWriter;

  private SmallIntWriter getSmallIntWriter() {
    if (smallIntWriter == null) {
      smallIntWriter = new NullableSmallIntWriterImpl(data.getSmallIntVector(), null);
      smallIntWriter.setPosition(idx());
      writers.add(smallIntWriter);
    }
    return smallIntWriter;
  }

  public SmallIntWriter asSmallInt() {
    data.getMutator().setType(idx(), MinorType.SMALLINT);
    return getSmallIntWriter();
  }

  @Override
  public void write(SmallIntHolder holder) {
    data.getMutator().setType(idx(), MinorType.SMALLINT);
    getSmallIntWriter().setPosition(idx());
    getSmallIntWriter().writeSmallInt(holder.value);
  }

  public void writeSmallInt(short value) {
    data.getMutator().setType(idx(), MinorType.SMALLINT);
    getSmallIntWriter().setPosition(idx());
    getSmallIntWriter().writeSmallInt(value);
  }



  private IntWriter intWriter;

  private IntWriter getIntWriter() {
    if (intWriter == null) {
      intWriter = new NullableIntWriterImpl(data.getIntVector(), null);
      intWriter.setPosition(idx());
      writers.add(intWriter);
    }
    return intWriter;
  }

  public IntWriter asInt() {
    data.getMutator().setType(idx(), MinorType.INT);
    return getIntWriter();
  }

  @Override
  public void write(IntHolder holder) {
    data.getMutator().setType(idx(), MinorType.INT);
    getIntWriter().setPosition(idx());
    getIntWriter().writeInt(holder.value);
  }

  public void writeInt(int value) {
    data.getMutator().setType(idx(), MinorType.INT);
    getIntWriter().setPosition(idx());
    getIntWriter().writeInt(value);
  }



  private UInt4Writer uInt4Writer;

  private UInt4Writer getUInt4Writer() {
    if (uInt4Writer == null) {
      uInt4Writer = new NullableUInt4WriterImpl(data.getUInt4Vector(), null);
      uInt4Writer.setPosition(idx());
      writers.add(uInt4Writer);
    }
    return uInt4Writer;
  }

  public UInt4Writer asUInt4() {
    data.getMutator().setType(idx(), MinorType.UINT4);
    return getUInt4Writer();
  }

  @Override
  public void write(UInt4Holder holder) {
    data.getMutator().setType(idx(), MinorType.UINT4);
    getUInt4Writer().setPosition(idx());
    getUInt4Writer().writeUInt4(holder.value);
  }

  public void writeUInt4(int value) {
    data.getMutator().setType(idx(), MinorType.UINT4);
    getUInt4Writer().setPosition(idx());
    getUInt4Writer().writeUInt4(value);
  }



  private Float4Writer float4Writer;

  private Float4Writer getFloat4Writer() {
    if (float4Writer == null) {
      float4Writer = new NullableFloat4WriterImpl(data.getFloat4Vector(), null);
      float4Writer.setPosition(idx());
      writers.add(float4Writer);
    }
    return float4Writer;
  }

  public Float4Writer asFloat4() {
    data.getMutator().setType(idx(), MinorType.FLOAT4);
    return getFloat4Writer();
  }

  @Override
  public void write(Float4Holder holder) {
    data.getMutator().setType(idx(), MinorType.FLOAT4);
    getFloat4Writer().setPosition(idx());
    getFloat4Writer().writeFloat4(holder.value);
  }

  public void writeFloat4(float value) {
    data.getMutator().setType(idx(), MinorType.FLOAT4);
    getFloat4Writer().setPosition(idx());
    getFloat4Writer().writeFloat4(value);
  }



  private TimeWriter timeWriter;

  private TimeWriter getTimeWriter() {
    if (timeWriter == null) {
      timeWriter = new NullableTimeWriterImpl(data.getTimeVector(), null);
      timeWriter.setPosition(idx());
      writers.add(timeWriter);
    }
    return timeWriter;
  }

  public TimeWriter asTime() {
    data.getMutator().setType(idx(), MinorType.TIME);
    return getTimeWriter();
  }

  @Override
  public void write(TimeHolder holder) {
    data.getMutator().setType(idx(), MinorType.TIME);
    getTimeWriter().setPosition(idx());
    getTimeWriter().writeTime(holder.value);
  }

  public void writeTime(int value) {
    data.getMutator().setType(idx(), MinorType.TIME);
    getTimeWriter().setPosition(idx());
    getTimeWriter().writeTime(value);
  }



  private IntervalYearWriter intervalYearWriter;

  private IntervalYearWriter getIntervalYearWriter() {
    if (intervalYearWriter == null) {
      intervalYearWriter = new NullableIntervalYearWriterImpl(data.getIntervalYearVector(), null);
      intervalYearWriter.setPosition(idx());
      writers.add(intervalYearWriter);
    }
    return intervalYearWriter;
  }

  public IntervalYearWriter asIntervalYear() {
    data.getMutator().setType(idx(), MinorType.INTERVALYEAR);
    return getIntervalYearWriter();
  }

  @Override
  public void write(IntervalYearHolder holder) {
    data.getMutator().setType(idx(), MinorType.INTERVALYEAR);
    getIntervalYearWriter().setPosition(idx());
    getIntervalYearWriter().writeIntervalYear(holder.value);
  }

  public void writeIntervalYear(int value) {
    data.getMutator().setType(idx(), MinorType.INTERVALYEAR);
    getIntervalYearWriter().setPosition(idx());
    getIntervalYearWriter().writeIntervalYear(value);
  }





  private BigIntWriter bigIntWriter;

  private BigIntWriter getBigIntWriter() {
    if (bigIntWriter == null) {
      bigIntWriter = new NullableBigIntWriterImpl(data.getBigIntVector(), null);
      bigIntWriter.setPosition(idx());
      writers.add(bigIntWriter);
    }
    return bigIntWriter;
  }

  public BigIntWriter asBigInt() {
    data.getMutator().setType(idx(), MinorType.BIGINT);
    return getBigIntWriter();
  }

  @Override
  public void write(BigIntHolder holder) {
    data.getMutator().setType(idx(), MinorType.BIGINT);
    getBigIntWriter().setPosition(idx());
    getBigIntWriter().writeBigInt(holder.value);
  }

  public void writeBigInt(long value) {
    data.getMutator().setType(idx(), MinorType.BIGINT);
    getBigIntWriter().setPosition(idx());
    getBigIntWriter().writeBigInt(value);
  }



  private UInt8Writer uInt8Writer;

  private UInt8Writer getUInt8Writer() {
    if (uInt8Writer == null) {
      uInt8Writer = new NullableUInt8WriterImpl(data.getUInt8Vector(), null);
      uInt8Writer.setPosition(idx());
      writers.add(uInt8Writer);
    }
    return uInt8Writer;
  }

  public UInt8Writer asUInt8() {
    data.getMutator().setType(idx(), MinorType.UINT8);
    return getUInt8Writer();
  }

  @Override
  public void write(UInt8Holder holder) {
    data.getMutator().setType(idx(), MinorType.UINT8);
    getUInt8Writer().setPosition(idx());
    getUInt8Writer().writeUInt8(holder.value);
  }

  public void writeUInt8(long value) {
    data.getMutator().setType(idx(), MinorType.UINT8);
    getUInt8Writer().setPosition(idx());
    getUInt8Writer().writeUInt8(value);
  }



  private Float8Writer float8Writer;

  private Float8Writer getFloat8Writer() {
    if (float8Writer == null) {
      float8Writer = new NullableFloat8WriterImpl(data.getFloat8Vector(), null);
      float8Writer.setPosition(idx());
      writers.add(float8Writer);
    }
    return float8Writer;
  }

  public Float8Writer asFloat8() {
    data.getMutator().setType(idx(), MinorType.FLOAT8);
    return getFloat8Writer();
  }

  @Override
  public void write(Float8Holder holder) {
    data.getMutator().setType(idx(), MinorType.FLOAT8);
    getFloat8Writer().setPosition(idx());
    getFloat8Writer().writeFloat8(holder.value);
  }

  public void writeFloat8(double value) {
    data.getMutator().setType(idx(), MinorType.FLOAT8);
    getFloat8Writer().setPosition(idx());
    getFloat8Writer().writeFloat8(value);
  }



  private DateWriter dateWriter;

  private DateWriter getDateWriter() {
    if (dateWriter == null) {
      dateWriter = new NullableDateWriterImpl(data.getDateVector(), null);
      dateWriter.setPosition(idx());
      writers.add(dateWriter);
    }
    return dateWriter;
  }

  public DateWriter asDate() {
    data.getMutator().setType(idx(), MinorType.DATE);
    return getDateWriter();
  }

  @Override
  public void write(DateHolder holder) {
    data.getMutator().setType(idx(), MinorType.DATE);
    getDateWriter().setPosition(idx());
    getDateWriter().writeDate(holder.value);
  }

  public void writeDate(long value) {
    data.getMutator().setType(idx(), MinorType.DATE);
    getDateWriter().setPosition(idx());
    getDateWriter().writeDate(value);
  }



  private TimeStampWriter timeStampWriter;

  private TimeStampWriter getTimeStampWriter() {
    if (timeStampWriter == null) {
      timeStampWriter = new NullableTimeStampWriterImpl(data.getTimeStampVector(), null);
      timeStampWriter.setPosition(idx());
      writers.add(timeStampWriter);
    }
    return timeStampWriter;
  }

  public TimeStampWriter asTimeStamp() {
    data.getMutator().setType(idx(), MinorType.TIMESTAMP);
    return getTimeStampWriter();
  }

  @Override
  public void write(TimeStampHolder holder) {
    data.getMutator().setType(idx(), MinorType.TIMESTAMP);
    getTimeStampWriter().setPosition(idx());
    getTimeStampWriter().writeTimeStamp(holder.value);
  }

  public void writeTimeStamp(long value) {
    data.getMutator().setType(idx(), MinorType.TIMESTAMP);
    getTimeStampWriter().setPosition(idx());
    getTimeStampWriter().writeTimeStamp(value);
  }





  private IntervalDayWriter intervalDayWriter;

  private IntervalDayWriter getIntervalDayWriter() {
    if (intervalDayWriter == null) {
      intervalDayWriter = new NullableIntervalDayWriterImpl(data.getIntervalDayVector(), null);
      intervalDayWriter.setPosition(idx());
      writers.add(intervalDayWriter);
    }
    return intervalDayWriter;
  }

  public IntervalDayWriter asIntervalDay() {
    data.getMutator().setType(idx(), MinorType.INTERVALDAY);
    return getIntervalDayWriter();
  }

  @Override
  public void write(IntervalDayHolder holder) {
    data.getMutator().setType(idx(), MinorType.INTERVALDAY);
    getIntervalDayWriter().setPosition(idx());
    getIntervalDayWriter().writeIntervalDay(holder.days, holder.milliseconds);
  }

  public void writeIntervalDay(int days, int milliseconds) {
    data.getMutator().setType(idx(), MinorType.INTERVALDAY);
    getIntervalDayWriter().setPosition(idx());
    getIntervalDayWriter().writeIntervalDay(days, milliseconds);
  }



  private IntervalWriter intervalWriter;

  private IntervalWriter getIntervalWriter() {
    if (intervalWriter == null) {
      intervalWriter = new NullableIntervalWriterImpl(data.getIntervalVector(), null);
      intervalWriter.setPosition(idx());
      writers.add(intervalWriter);
    }
    return intervalWriter;
  }

  public IntervalWriter asInterval() {
    data.getMutator().setType(idx(), MinorType.INTERVAL);
    return getIntervalWriter();
  }

  @Override
  public void write(IntervalHolder holder) {
    data.getMutator().setType(idx(), MinorType.INTERVAL);
    getIntervalWriter().setPosition(idx());
    getIntervalWriter().writeInterval(holder.months, holder.days, holder.milliseconds);
  }

  public void writeInterval(int months, int days, int milliseconds) {
    data.getMutator().setType(idx(), MinorType.INTERVAL);
    getIntervalWriter().setPosition(idx());
    getIntervalWriter().writeInterval(months, days, milliseconds);
  }











  private VarBinaryWriter varBinaryWriter;

  private VarBinaryWriter getVarBinaryWriter() {
    if (varBinaryWriter == null) {
      varBinaryWriter = new NullableVarBinaryWriterImpl(data.getVarBinaryVector(), null);
      varBinaryWriter.setPosition(idx());
      writers.add(varBinaryWriter);
    }
    return varBinaryWriter;
  }

  public VarBinaryWriter asVarBinary() {
    data.getMutator().setType(idx(), MinorType.VARBINARY);
    return getVarBinaryWriter();
  }

  @Override
  public void write(VarBinaryHolder holder) {
    data.getMutator().setType(idx(), MinorType.VARBINARY);
    getVarBinaryWriter().setPosition(idx());
    getVarBinaryWriter().writeVarBinary(holder.start, holder.end, holder.buffer);
  }

  public void writeVarBinary(int start, int end, ArrowBuf buffer) {
    data.getMutator().setType(idx(), MinorType.VARBINARY);
    getVarBinaryWriter().setPosition(idx());
    getVarBinaryWriter().writeVarBinary(start, end, buffer);
  }



  private VarCharWriter varCharWriter;

  private VarCharWriter getVarCharWriter() {
    if (varCharWriter == null) {
      varCharWriter = new NullableVarCharWriterImpl(data.getVarCharVector(), null);
      varCharWriter.setPosition(idx());
      writers.add(varCharWriter);
    }
    return varCharWriter;
  }

  public VarCharWriter asVarChar() {
    data.getMutator().setType(idx(), MinorType.VARCHAR);
    return getVarCharWriter();
  }

  @Override
  public void write(VarCharHolder holder) {
    data.getMutator().setType(idx(), MinorType.VARCHAR);
    getVarCharWriter().setPosition(idx());
    getVarCharWriter().writeVarChar(holder.start, holder.end, holder.buffer);
  }

  public void writeVarChar(int start, int end, ArrowBuf buffer) {
    data.getMutator().setType(idx(), MinorType.VARCHAR);
    getVarCharWriter().setPosition(idx());
    getVarCharWriter().writeVarChar(start, end, buffer);
  }



  private Var16CharWriter var16CharWriter;

  private Var16CharWriter getVar16CharWriter() {
    if (var16CharWriter == null) {
      var16CharWriter = new NullableVar16CharWriterImpl(data.getVar16CharVector(), null);
      var16CharWriter.setPosition(idx());
      writers.add(var16CharWriter);
    }
    return var16CharWriter;
  }

  public Var16CharWriter asVar16Char() {
    data.getMutator().setType(idx(), MinorType.VAR16CHAR);
    return getVar16CharWriter();
  }

  @Override
  public void write(Var16CharHolder holder) {
    data.getMutator().setType(idx(), MinorType.VAR16CHAR);
    getVar16CharWriter().setPosition(idx());
    getVar16CharWriter().writeVar16Char(holder.start, holder.end, holder.buffer);
  }

  public void writeVar16Char(int start, int end, ArrowBuf buffer) {
    data.getMutator().setType(idx(), MinorType.VAR16CHAR);
    getVar16CharWriter().setPosition(idx());
    getVar16CharWriter().writeVar16Char(start, end, buffer);
  }



  private BitWriter bitWriter;

  private BitWriter getBitWriter() {
    if (bitWriter == null) {
      bitWriter = new NullableBitWriterImpl(data.getBitVector(), null);
      bitWriter.setPosition(idx());
      writers.add(bitWriter);
    }
    return bitWriter;
  }

  public BitWriter asBit() {
    data.getMutator().setType(idx(), MinorType.BIT);
    return getBitWriter();
  }

  @Override
  public void write(BitHolder holder) {
    data.getMutator().setType(idx(), MinorType.BIT);
    getBitWriter().setPosition(idx());
    getBitWriter().writeBit(holder.value);
  }

  public void writeBit(int value) {
    data.getMutator().setType(idx(), MinorType.BIT);
    getBitWriter().setPosition(idx());
    getBitWriter().writeBit(value);
  }


  public void writeNull() {
  }

  @Override
  public MapWriter map() {
    data.getMutator().setType(idx(), MinorType.LIST);
    getListWriter().setPosition(idx());
    return getListWriter().map();
  }

  @Override
  public ListWriter list() {
    data.getMutator().setType(idx(), MinorType.LIST);
    getListWriter().setPosition(idx());
    return getListWriter().list();
  }

  @Override
  public ListWriter list(String name) {
    data.getMutator().setType(idx(), MinorType.MAP);
    getMapWriter().setPosition(idx());
    return getMapWriter().list(name);
  }

  @Override
  public MapWriter map(String name) {
    data.getMutator().setType(idx(), MinorType.MAP);
    getMapWriter().setPosition(idx());
    return getMapWriter().map(name);
  }

  @Override
  public TinyIntWriter tinyInt(String name) {
    data.getMutator().setType(idx(), MinorType.MAP);
    getMapWriter().setPosition(idx());
    return getMapWriter().tinyInt(name);
  }

  @Override
  public TinyIntWriter tinyInt() {
    data.getMutator().setType(idx(), MinorType.LIST);
    getListWriter().setPosition(idx());
    return getListWriter().tinyInt();
  }
  @Override
  public UInt1Writer uInt1(String name) {
    data.getMutator().setType(idx(), MinorType.MAP);
    getMapWriter().setPosition(idx());
    return getMapWriter().uInt1(name);
  }

  @Override
  public UInt1Writer uInt1() {
    data.getMutator().setType(idx(), MinorType.LIST);
    getListWriter().setPosition(idx());
    return getListWriter().uInt1();
  }
  @Override
  public UInt2Writer uInt2(String name) {
    data.getMutator().setType(idx(), MinorType.MAP);
    getMapWriter().setPosition(idx());
    return getMapWriter().uInt2(name);
  }

  @Override
  public UInt2Writer uInt2() {
    data.getMutator().setType(idx(), MinorType.LIST);
    getListWriter().setPosition(idx());
    return getListWriter().uInt2();
  }
  @Override
  public SmallIntWriter smallInt(String name) {
    data.getMutator().setType(idx(), MinorType.MAP);
    getMapWriter().setPosition(idx());
    return getMapWriter().smallInt(name);
  }

  @Override
  public SmallIntWriter smallInt() {
    data.getMutator().setType(idx(), MinorType.LIST);
    getListWriter().setPosition(idx());
    return getListWriter().smallInt();
  }
  @Override
  public IntWriter integer(String name) {
    data.getMutator().setType(idx(), MinorType.MAP);
    getMapWriter().setPosition(idx());
    return getMapWriter().integer(name);
  }

  @Override
  public IntWriter integer() {
    data.getMutator().setType(idx(), MinorType.LIST);
    getListWriter().setPosition(idx());
    return getListWriter().integer();
  }
  @Override
  public UInt4Writer uInt4(String name) {
    data.getMutator().setType(idx(), MinorType.MAP);
    getMapWriter().setPosition(idx());
    return getMapWriter().uInt4(name);
  }

  @Override
  public UInt4Writer uInt4() {
    data.getMutator().setType(idx(), MinorType.LIST);
    getListWriter().setPosition(idx());
    return getListWriter().uInt4();
  }
  @Override
  public Float4Writer float4(String name) {
    data.getMutator().setType(idx(), MinorType.MAP);
    getMapWriter().setPosition(idx());
    return getMapWriter().float4(name);
  }

  @Override
  public Float4Writer float4() {
    data.getMutator().setType(idx(), MinorType.LIST);
    getListWriter().setPosition(idx());
    return getListWriter().float4();
  }
  @Override
  public TimeWriter time(String name) {
    data.getMutator().setType(idx(), MinorType.MAP);
    getMapWriter().setPosition(idx());
    return getMapWriter().time(name);
  }

  @Override
  public TimeWriter time() {
    data.getMutator().setType(idx(), MinorType.LIST);
    getListWriter().setPosition(idx());
    return getListWriter().time();
  }
  @Override
  public IntervalYearWriter intervalYear(String name) {
    data.getMutator().setType(idx(), MinorType.MAP);
    getMapWriter().setPosition(idx());
    return getMapWriter().intervalYear(name);
  }

  @Override
  public IntervalYearWriter intervalYear() {
    data.getMutator().setType(idx(), MinorType.LIST);
    getListWriter().setPosition(idx());
    return getListWriter().intervalYear();
  }
  @Override
  public BigIntWriter bigInt(String name) {
    data.getMutator().setType(idx(), MinorType.MAP);
    getMapWriter().setPosition(idx());
    return getMapWriter().bigInt(name);
  }

  @Override
  public BigIntWriter bigInt() {
    data.getMutator().setType(idx(), MinorType.LIST);
    getListWriter().setPosition(idx());
    return getListWriter().bigInt();
  }
  @Override
  public UInt8Writer uInt8(String name) {
    data.getMutator().setType(idx(), MinorType.MAP);
    getMapWriter().setPosition(idx());
    return getMapWriter().uInt8(name);
  }

  @Override
  public UInt8Writer uInt8() {
    data.getMutator().setType(idx(), MinorType.LIST);
    getListWriter().setPosition(idx());
    return getListWriter().uInt8();
  }
  @Override
  public Float8Writer float8(String name) {
    data.getMutator().setType(idx(), MinorType.MAP);
    getMapWriter().setPosition(idx());
    return getMapWriter().float8(name);
  }

  @Override
  public Float8Writer float8() {
    data.getMutator().setType(idx(), MinorType.LIST);
    getListWriter().setPosition(idx());
    return getListWriter().float8();
  }
  @Override
  public DateWriter date(String name) {
    data.getMutator().setType(idx(), MinorType.MAP);
    getMapWriter().setPosition(idx());
    return getMapWriter().date(name);
  }

  @Override
  public DateWriter date() {
    data.getMutator().setType(idx(), MinorType.LIST);
    getListWriter().setPosition(idx());
    return getListWriter().date();
  }
  @Override
  public TimeStampWriter timeStamp(String name) {
    data.getMutator().setType(idx(), MinorType.MAP);
    getMapWriter().setPosition(idx());
    return getMapWriter().timeStamp(name);
  }

  @Override
  public TimeStampWriter timeStamp() {
    data.getMutator().setType(idx(), MinorType.LIST);
    getListWriter().setPosition(idx());
    return getListWriter().timeStamp();
  }
  @Override
  public IntervalDayWriter intervalDay(String name) {
    data.getMutator().setType(idx(), MinorType.MAP);
    getMapWriter().setPosition(idx());
    return getMapWriter().intervalDay(name);
  }

  @Override
  public IntervalDayWriter intervalDay() {
    data.getMutator().setType(idx(), MinorType.LIST);
    getListWriter().setPosition(idx());
    return getListWriter().intervalDay();
  }
  @Override
  public IntervalWriter interval(String name) {
    data.getMutator().setType(idx(), MinorType.MAP);
    getMapWriter().setPosition(idx());
    return getMapWriter().interval(name);
  }

  @Override
  public IntervalWriter interval() {
    data.getMutator().setType(idx(), MinorType.LIST);
    getListWriter().setPosition(idx());
    return getListWriter().interval();
  }
  @Override
  public VarBinaryWriter varBinary(String name) {
    data.getMutator().setType(idx(), MinorType.MAP);
    getMapWriter().setPosition(idx());
    return getMapWriter().varBinary(name);
  }

  @Override
  public VarBinaryWriter varBinary() {
    data.getMutator().setType(idx(), MinorType.LIST);
    getListWriter().setPosition(idx());
    return getListWriter().varBinary();
  }
  @Override
  public VarCharWriter varChar(String name) {
    data.getMutator().setType(idx(), MinorType.MAP);
    getMapWriter().setPosition(idx());
    return getMapWriter().varChar(name);
  }

  @Override
  public VarCharWriter varChar() {
    data.getMutator().setType(idx(), MinorType.LIST);
    getListWriter().setPosition(idx());
    return getListWriter().varChar();
  }
  @Override
  public Var16CharWriter var16Char(String name) {
    data.getMutator().setType(idx(), MinorType.MAP);
    getMapWriter().setPosition(idx());
    return getMapWriter().var16Char(name);
  }

  @Override
  public Var16CharWriter var16Char() {
    data.getMutator().setType(idx(), MinorType.LIST);
    getListWriter().setPosition(idx());
    return getListWriter().var16Char();
  }
  @Override
  public BitWriter bit(String name) {
    data.getMutator().setType(idx(), MinorType.MAP);
    getMapWriter().setPosition(idx());
    return getMapWriter().bit(name);
  }

  @Override
  public BitWriter bit() {
    data.getMutator().setType(idx(), MinorType.LIST);
    getListWriter().setPosition(idx());
    return getListWriter().bit();
  }

  @Override
  public void allocate() {
    data.allocateNew();
  }

  @Override
  public void clear() {
    data.clear();
  }

  @Override
  public void close() throws Exception {
    data.close();
  }

  @Override
  public MaterializedField getField() {
    return data.getField();
  }

  @Override
  public int getValueCapacity() {
    return data.getValueCapacity();
  }
}
