

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







@SuppressWarnings("unused")
public class UnionReader extends AbstractFieldReader {

  private BaseReader[] readers = new BaseReader[43];
  public UnionVector data;
  
  public UnionReader(UnionVector data) {
    this.data = data;
  }

  private static MajorType[] TYPES = new MajorType[43];

  static {
    for (MinorType minorType : MinorType.values()) {
      TYPES[minorType.ordinal()] = new MajorType(minorType, DataMode.OPTIONAL);
    }
  }

  public MajorType getType() {
    return TYPES[data.getTypeValue(idx())];
  }

  public boolean isSet(){
    return !data.getAccessor().isNull(idx());
  }

  public void read(UnionHolder holder) {
    holder.reader = this;
    holder.isSet = this.isSet() ? 1 : 0;
  }

  public void read(int index, UnionHolder holder) {
    getList().read(index, holder);
  }

  private FieldReader getReaderForIndex(int index) {
    int typeValue = data.getTypeValue(index);
    FieldReader reader = (FieldReader) readers[typeValue];
    if (reader != null) {
      return reader;
    }
    switch (MinorType.values()[typeValue]) {
    case LATE:
      return NullReader.INSTANCE;
    case MAP:
      return (FieldReader) getMap();
    case LIST:
      return (FieldReader) getList();
    case TINYINT:
      return (FieldReader) getTinyInt();
    case UINT1:
      return (FieldReader) getUInt1();
    case UINT2:
      return (FieldReader) getUInt2();
    case SMALLINT:
      return (FieldReader) getSmallInt();
    case INT:
      return (FieldReader) getInt();
    case UINT4:
      return (FieldReader) getUInt4();
    case FLOAT4:
      return (FieldReader) getFloat4();
    case TIME:
      return (FieldReader) getTime();
    case INTERVALYEAR:
      return (FieldReader) getIntervalYear();
    case BIGINT:
      return (FieldReader) getBigInt();
    case UINT8:
      return (FieldReader) getUInt8();
    case FLOAT8:
      return (FieldReader) getFloat8();
    case DATE:
      return (FieldReader) getDate();
    case TIMESTAMP:
      return (FieldReader) getTimeStamp();
    case INTERVALDAY:
      return (FieldReader) getIntervalDay();
    case INTERVAL:
      return (FieldReader) getInterval();
    case VARBINARY:
      return (FieldReader) getVarBinary();
    case VARCHAR:
      return (FieldReader) getVarChar();
    case VAR16CHAR:
      return (FieldReader) getVar16Char();
    case BIT:
      return (FieldReader) getBit();
    default:
      throw new UnsupportedOperationException("Unsupported type: " + MinorType.values()[typeValue]);
    }
  }

  private SingleMapReaderImpl mapReader;

  private MapReader getMap() {
    if (mapReader == null) {
      mapReader = (SingleMapReaderImpl) data.getMap().getReader();
      mapReader.setPosition(idx());
      readers[MinorType.MAP.ordinal()] = mapReader;
    }
    return mapReader;
  }

  private UnionListReader listReader;

  private FieldReader getList() {
    if (listReader == null) {
      listReader = new UnionListReader(data.getList());
      listReader.setPosition(idx());
      readers[MinorType.LIST.ordinal()] = listReader;
    }
    return listReader;
  }

  @Override
  public java.util.Iterator<String> iterator() {
    return getMap().iterator();
  }

  @Override
  public void copyAsValue(UnionWriter writer) {
    writer.data.copyFrom(idx(), writer.idx(), data);
  }


  @Override
  public Object readObject() {
    return getReaderForIndex(idx()).readObject();
  }


  @Override
  public BigDecimal readBigDecimal() {
    return getReaderForIndex(idx()).readBigDecimal();
  }


  @Override
  public Integer readInteger() {
    return getReaderForIndex(idx()).readInteger();
  }


  @Override
  public Long readLong() {
    return getReaderForIndex(idx()).readLong();
  }


  @Override
  public Boolean readBoolean() {
    return getReaderForIndex(idx()).readBoolean();
  }


  @Override
  public Character readCharacter() {
    return getReaderForIndex(idx()).readCharacter();
  }


  @Override
  public DateTime readDateTime() {
    return getReaderForIndex(idx()).readDateTime();
  }


  @Override
  public Period readPeriod() {
    return getReaderForIndex(idx()).readPeriod();
  }


  @Override
  public Double readDouble() {
    return getReaderForIndex(idx()).readDouble();
  }


  @Override
  public Float readFloat() {
    return getReaderForIndex(idx()).readFloat();
  }


  @Override
  public Text readText() {
    return getReaderForIndex(idx()).readText();
  }


  @Override
  public String readString() {
    return getReaderForIndex(idx()).readString();
  }


  @Override
  public Byte readByte() {
    return getReaderForIndex(idx()).readByte();
  }


  @Override
  public Short readShort() {
    return getReaderForIndex(idx()).readShort();
  }


  @Override
  public byte[] readByteArray() {
    return getReaderForIndex(idx()).readByteArray();
  }



  private NullableTinyIntReaderImpl tinyIntReader;

  private NullableTinyIntReaderImpl getTinyInt() {
    if (tinyIntReader == null) {
      tinyIntReader = new NullableTinyIntReaderImpl(data.getTinyIntVector());
      tinyIntReader.setPosition(idx());
      readers[MinorType.TINYINT.ordinal()] = tinyIntReader;
    }
    return tinyIntReader;
  }

  public void read(NullableTinyIntHolder holder){
    getReaderForIndex(idx()).read(holder);
  }

  public void copyAsValue(TinyIntWriter writer){
    getReaderForIndex(idx()).copyAsValue(writer);
  }

  private NullableUInt1ReaderImpl uInt1Reader;

  private NullableUInt1ReaderImpl getUInt1() {
    if (uInt1Reader == null) {
      uInt1Reader = new NullableUInt1ReaderImpl(data.getUInt1Vector());
      uInt1Reader.setPosition(idx());
      readers[MinorType.UINT1.ordinal()] = uInt1Reader;
    }
    return uInt1Reader;
  }

  public void read(NullableUInt1Holder holder){
    getReaderForIndex(idx()).read(holder);
  }

  public void copyAsValue(UInt1Writer writer){
    getReaderForIndex(idx()).copyAsValue(writer);
  }

  private NullableUInt2ReaderImpl uInt2Reader;

  private NullableUInt2ReaderImpl getUInt2() {
    if (uInt2Reader == null) {
      uInt2Reader = new NullableUInt2ReaderImpl(data.getUInt2Vector());
      uInt2Reader.setPosition(idx());
      readers[MinorType.UINT2.ordinal()] = uInt2Reader;
    }
    return uInt2Reader;
  }

  public void read(NullableUInt2Holder holder){
    getReaderForIndex(idx()).read(holder);
  }

  public void copyAsValue(UInt2Writer writer){
    getReaderForIndex(idx()).copyAsValue(writer);
  }

  private NullableSmallIntReaderImpl smallIntReader;

  private NullableSmallIntReaderImpl getSmallInt() {
    if (smallIntReader == null) {
      smallIntReader = new NullableSmallIntReaderImpl(data.getSmallIntVector());
      smallIntReader.setPosition(idx());
      readers[MinorType.SMALLINT.ordinal()] = smallIntReader;
    }
    return smallIntReader;
  }

  public void read(NullableSmallIntHolder holder){
    getReaderForIndex(idx()).read(holder);
  }

  public void copyAsValue(SmallIntWriter writer){
    getReaderForIndex(idx()).copyAsValue(writer);
  }

  private NullableIntReaderImpl intReader;

  private NullableIntReaderImpl getInt() {
    if (intReader == null) {
      intReader = new NullableIntReaderImpl(data.getIntVector());
      intReader.setPosition(idx());
      readers[MinorType.INT.ordinal()] = intReader;
    }
    return intReader;
  }

  public void read(NullableIntHolder holder){
    getReaderForIndex(idx()).read(holder);
  }

  public void copyAsValue(IntWriter writer){
    getReaderForIndex(idx()).copyAsValue(writer);
  }

  private NullableUInt4ReaderImpl uInt4Reader;

  private NullableUInt4ReaderImpl getUInt4() {
    if (uInt4Reader == null) {
      uInt4Reader = new NullableUInt4ReaderImpl(data.getUInt4Vector());
      uInt4Reader.setPosition(idx());
      readers[MinorType.UINT4.ordinal()] = uInt4Reader;
    }
    return uInt4Reader;
  }

  public void read(NullableUInt4Holder holder){
    getReaderForIndex(idx()).read(holder);
  }

  public void copyAsValue(UInt4Writer writer){
    getReaderForIndex(idx()).copyAsValue(writer);
  }

  private NullableFloat4ReaderImpl float4Reader;

  private NullableFloat4ReaderImpl getFloat4() {
    if (float4Reader == null) {
      float4Reader = new NullableFloat4ReaderImpl(data.getFloat4Vector());
      float4Reader.setPosition(idx());
      readers[MinorType.FLOAT4.ordinal()] = float4Reader;
    }
    return float4Reader;
  }

  public void read(NullableFloat4Holder holder){
    getReaderForIndex(idx()).read(holder);
  }

  public void copyAsValue(Float4Writer writer){
    getReaderForIndex(idx()).copyAsValue(writer);
  }

  private NullableTimeReaderImpl timeReader;

  private NullableTimeReaderImpl getTime() {
    if (timeReader == null) {
      timeReader = new NullableTimeReaderImpl(data.getTimeVector());
      timeReader.setPosition(idx());
      readers[MinorType.TIME.ordinal()] = timeReader;
    }
    return timeReader;
  }

  public void read(NullableTimeHolder holder){
    getReaderForIndex(idx()).read(holder);
  }

  public void copyAsValue(TimeWriter writer){
    getReaderForIndex(idx()).copyAsValue(writer);
  }

  private NullableIntervalYearReaderImpl intervalYearReader;

  private NullableIntervalYearReaderImpl getIntervalYear() {
    if (intervalYearReader == null) {
      intervalYearReader = new NullableIntervalYearReaderImpl(data.getIntervalYearVector());
      intervalYearReader.setPosition(idx());
      readers[MinorType.INTERVALYEAR.ordinal()] = intervalYearReader;
    }
    return intervalYearReader;
  }

  public void read(NullableIntervalYearHolder holder){
    getReaderForIndex(idx()).read(holder);
  }

  public void copyAsValue(IntervalYearWriter writer){
    getReaderForIndex(idx()).copyAsValue(writer);
  }

  private NullableBigIntReaderImpl bigIntReader;

  private NullableBigIntReaderImpl getBigInt() {
    if (bigIntReader == null) {
      bigIntReader = new NullableBigIntReaderImpl(data.getBigIntVector());
      bigIntReader.setPosition(idx());
      readers[MinorType.BIGINT.ordinal()] = bigIntReader;
    }
    return bigIntReader;
  }

  public void read(NullableBigIntHolder holder){
    getReaderForIndex(idx()).read(holder);
  }

  public void copyAsValue(BigIntWriter writer){
    getReaderForIndex(idx()).copyAsValue(writer);
  }

  private NullableUInt8ReaderImpl uInt8Reader;

  private NullableUInt8ReaderImpl getUInt8() {
    if (uInt8Reader == null) {
      uInt8Reader = new NullableUInt8ReaderImpl(data.getUInt8Vector());
      uInt8Reader.setPosition(idx());
      readers[MinorType.UINT8.ordinal()] = uInt8Reader;
    }
    return uInt8Reader;
  }

  public void read(NullableUInt8Holder holder){
    getReaderForIndex(idx()).read(holder);
  }

  public void copyAsValue(UInt8Writer writer){
    getReaderForIndex(idx()).copyAsValue(writer);
  }

  private NullableFloat8ReaderImpl float8Reader;

  private NullableFloat8ReaderImpl getFloat8() {
    if (float8Reader == null) {
      float8Reader = new NullableFloat8ReaderImpl(data.getFloat8Vector());
      float8Reader.setPosition(idx());
      readers[MinorType.FLOAT8.ordinal()] = float8Reader;
    }
    return float8Reader;
  }

  public void read(NullableFloat8Holder holder){
    getReaderForIndex(idx()).read(holder);
  }

  public void copyAsValue(Float8Writer writer){
    getReaderForIndex(idx()).copyAsValue(writer);
  }

  private NullableDateReaderImpl dateReader;

  private NullableDateReaderImpl getDate() {
    if (dateReader == null) {
      dateReader = new NullableDateReaderImpl(data.getDateVector());
      dateReader.setPosition(idx());
      readers[MinorType.DATE.ordinal()] = dateReader;
    }
    return dateReader;
  }

  public void read(NullableDateHolder holder){
    getReaderForIndex(idx()).read(holder);
  }

  public void copyAsValue(DateWriter writer){
    getReaderForIndex(idx()).copyAsValue(writer);
  }

  private NullableTimeStampReaderImpl timeStampReader;

  private NullableTimeStampReaderImpl getTimeStamp() {
    if (timeStampReader == null) {
      timeStampReader = new NullableTimeStampReaderImpl(data.getTimeStampVector());
      timeStampReader.setPosition(idx());
      readers[MinorType.TIMESTAMP.ordinal()] = timeStampReader;
    }
    return timeStampReader;
  }

  public void read(NullableTimeStampHolder holder){
    getReaderForIndex(idx()).read(holder);
  }

  public void copyAsValue(TimeStampWriter writer){
    getReaderForIndex(idx()).copyAsValue(writer);
  }

  private NullableIntervalDayReaderImpl intervalDayReader;

  private NullableIntervalDayReaderImpl getIntervalDay() {
    if (intervalDayReader == null) {
      intervalDayReader = new NullableIntervalDayReaderImpl(data.getIntervalDayVector());
      intervalDayReader.setPosition(idx());
      readers[MinorType.INTERVALDAY.ordinal()] = intervalDayReader;
    }
    return intervalDayReader;
  }

  public void read(NullableIntervalDayHolder holder){
    getReaderForIndex(idx()).read(holder);
  }

  public void copyAsValue(IntervalDayWriter writer){
    getReaderForIndex(idx()).copyAsValue(writer);
  }

  private NullableIntervalReaderImpl intervalReader;

  private NullableIntervalReaderImpl getInterval() {
    if (intervalReader == null) {
      intervalReader = new NullableIntervalReaderImpl(data.getIntervalVector());
      intervalReader.setPosition(idx());
      readers[MinorType.INTERVAL.ordinal()] = intervalReader;
    }
    return intervalReader;
  }

  public void read(NullableIntervalHolder holder){
    getReaderForIndex(idx()).read(holder);
  }

  public void copyAsValue(IntervalWriter writer){
    getReaderForIndex(idx()).copyAsValue(writer);
  }

  private NullableVarBinaryReaderImpl varBinaryReader;

  private NullableVarBinaryReaderImpl getVarBinary() {
    if (varBinaryReader == null) {
      varBinaryReader = new NullableVarBinaryReaderImpl(data.getVarBinaryVector());
      varBinaryReader.setPosition(idx());
      readers[MinorType.VARBINARY.ordinal()] = varBinaryReader;
    }
    return varBinaryReader;
  }

  public void read(NullableVarBinaryHolder holder){
    getReaderForIndex(idx()).read(holder);
  }

  public void copyAsValue(VarBinaryWriter writer){
    getReaderForIndex(idx()).copyAsValue(writer);
  }

  private NullableVarCharReaderImpl varCharReader;

  private NullableVarCharReaderImpl getVarChar() {
    if (varCharReader == null) {
      varCharReader = new NullableVarCharReaderImpl(data.getVarCharVector());
      varCharReader.setPosition(idx());
      readers[MinorType.VARCHAR.ordinal()] = varCharReader;
    }
    return varCharReader;
  }

  public void read(NullableVarCharHolder holder){
    getReaderForIndex(idx()).read(holder);
  }

  public void copyAsValue(VarCharWriter writer){
    getReaderForIndex(idx()).copyAsValue(writer);
  }

  private NullableVar16CharReaderImpl var16CharReader;

  private NullableVar16CharReaderImpl getVar16Char() {
    if (var16CharReader == null) {
      var16CharReader = new NullableVar16CharReaderImpl(data.getVar16CharVector());
      var16CharReader.setPosition(idx());
      readers[MinorType.VAR16CHAR.ordinal()] = var16CharReader;
    }
    return var16CharReader;
  }

  public void read(NullableVar16CharHolder holder){
    getReaderForIndex(idx()).read(holder);
  }

  public void copyAsValue(Var16CharWriter writer){
    getReaderForIndex(idx()).copyAsValue(writer);
  }

  private NullableBitReaderImpl bitReader;

  private NullableBitReaderImpl getBit() {
    if (bitReader == null) {
      bitReader = new NullableBitReaderImpl(data.getBitVector());
      bitReader.setPosition(idx());
      readers[MinorType.BIT.ordinal()] = bitReader;
    }
    return bitReader;
  }

  public void read(NullableBitHolder holder){
    getReaderForIndex(idx()).read(holder);
  }

  public void copyAsValue(BitWriter writer){
    getReaderForIndex(idx()).copyAsValue(writer);
  }

  @Override
  public void copyAsValue(ListWriter writer) {
    ComplexCopier.copy(this, (FieldWriter) writer);
  }

  @Override
  public void setPosition(int index) {
    super.setPosition(index);
    for (BaseReader reader : readers) {
      if (reader != null) {
        reader.setPosition(index);
      }
    }
  }
  
  public FieldReader reader(String name){
    return getMap().reader(name);
  }

  public FieldReader reader() {
    return getList().reader();
  }

  public boolean next() {
    return getReaderForIndex(idx()).next();
  }
}



