

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
abstract class AbstractFieldReader extends AbstractBaseReader implements FieldReader{
  
  AbstractFieldReader(){
    super();
  }

  /**
   * Returns true if the current value of the reader is not null
   * @return
   */
  public boolean isSet() {
    return true;
  }

  
  public Object readObject(int arrayIndex){
    fail("readObject(int arrayIndex)");
    return null;
  }
  
  public Object readObject(){
    fail("readObject()");
    return null;
  }
  
  
  public BigDecimal readBigDecimal(int arrayIndex){
    fail("readBigDecimal(int arrayIndex)");
    return null;
  }
  
  public BigDecimal readBigDecimal(){
    fail("readBigDecimal()");
    return null;
  }
  
  
  public Integer readInteger(int arrayIndex){
    fail("readInteger(int arrayIndex)");
    return null;
  }
  
  public Integer readInteger(){
    fail("readInteger()");
    return null;
  }
  
  
  public Long readLong(int arrayIndex){
    fail("readLong(int arrayIndex)");
    return null;
  }
  
  public Long readLong(){
    fail("readLong()");
    return null;
  }
  
  
  public Boolean readBoolean(int arrayIndex){
    fail("readBoolean(int arrayIndex)");
    return null;
  }
  
  public Boolean readBoolean(){
    fail("readBoolean()");
    return null;
  }
  
  
  public Character readCharacter(int arrayIndex){
    fail("readCharacter(int arrayIndex)");
    return null;
  }
  
  public Character readCharacter(){
    fail("readCharacter()");
    return null;
  }
  
  
  public DateTime readDateTime(int arrayIndex){
    fail("readDateTime(int arrayIndex)");
    return null;
  }
  
  public DateTime readDateTime(){
    fail("readDateTime()");
    return null;
  }
  
  
  public Period readPeriod(int arrayIndex){
    fail("readPeriod(int arrayIndex)");
    return null;
  }
  
  public Period readPeriod(){
    fail("readPeriod()");
    return null;
  }
  
  
  public Double readDouble(int arrayIndex){
    fail("readDouble(int arrayIndex)");
    return null;
  }
  
  public Double readDouble(){
    fail("readDouble()");
    return null;
  }
  
  
  public Float readFloat(int arrayIndex){
    fail("readFloat(int arrayIndex)");
    return null;
  }
  
  public Float readFloat(){
    fail("readFloat()");
    return null;
  }
  
  
  public Text readText(int arrayIndex){
    fail("readText(int arrayIndex)");
    return null;
  }
  
  public Text readText(){
    fail("readText()");
    return null;
  }
  
  
  public String readString(int arrayIndex){
    fail("readString(int arrayIndex)");
    return null;
  }
  
  public String readString(){
    fail("readString()");
    return null;
  }
  
  
  public Byte readByte(int arrayIndex){
    fail("readByte(int arrayIndex)");
    return null;
  }
  
  public Byte readByte(){
    fail("readByte()");
    return null;
  }
  
  
  public Short readShort(int arrayIndex){
    fail("readShort(int arrayIndex)");
    return null;
  }
  
  public Short readShort(){
    fail("readShort()");
    return null;
  }
  
  
  public byte[] readByteArray(int arrayIndex){
    fail("readByteArray(int arrayIndex)");
    return null;
  }
  
  public byte[] readByteArray(){
    fail("readByteArray()");
    return null;
  }
  
  
  public void copyAsValue(MapWriter writer){
    fail("CopyAsValue MapWriter");
  }
  public void copyAsField(String name, MapWriter writer){
    fail("CopyAsField MapWriter");
  }

  public void copyAsField(String name, ListWriter writer){
    fail("CopyAsFieldList");
  }
  

  public void read(TinyIntHolder holder){
    fail("TinyInt");
  }

  public void read(NullableTinyIntHolder holder){
    fail("TinyInt");
  }
  
  public void read(int arrayIndex, TinyIntHolder holder){
    fail("RepeatedTinyInt");
  }
  
  public void read(int arrayIndex, NullableTinyIntHolder holder){
    fail("RepeatedTinyInt");
  }
  
  public void copyAsValue(TinyIntWriter writer){
    fail("CopyAsValueTinyInt");
  }
  public void copyAsField(String name, TinyIntWriter writer){
    fail("CopyAsFieldTinyInt");
  }

  public void read(UInt1Holder holder){
    fail("UInt1");
  }

  public void read(NullableUInt1Holder holder){
    fail("UInt1");
  }
  
  public void read(int arrayIndex, UInt1Holder holder){
    fail("RepeatedUInt1");
  }
  
  public void read(int arrayIndex, NullableUInt1Holder holder){
    fail("RepeatedUInt1");
  }
  
  public void copyAsValue(UInt1Writer writer){
    fail("CopyAsValueUInt1");
  }
  public void copyAsField(String name, UInt1Writer writer){
    fail("CopyAsFieldUInt1");
  }

  public void read(UInt2Holder holder){
    fail("UInt2");
  }

  public void read(NullableUInt2Holder holder){
    fail("UInt2");
  }
  
  public void read(int arrayIndex, UInt2Holder holder){
    fail("RepeatedUInt2");
  }
  
  public void read(int arrayIndex, NullableUInt2Holder holder){
    fail("RepeatedUInt2");
  }
  
  public void copyAsValue(UInt2Writer writer){
    fail("CopyAsValueUInt2");
  }
  public void copyAsField(String name, UInt2Writer writer){
    fail("CopyAsFieldUInt2");
  }

  public void read(SmallIntHolder holder){
    fail("SmallInt");
  }

  public void read(NullableSmallIntHolder holder){
    fail("SmallInt");
  }
  
  public void read(int arrayIndex, SmallIntHolder holder){
    fail("RepeatedSmallInt");
  }
  
  public void read(int arrayIndex, NullableSmallIntHolder holder){
    fail("RepeatedSmallInt");
  }
  
  public void copyAsValue(SmallIntWriter writer){
    fail("CopyAsValueSmallInt");
  }
  public void copyAsField(String name, SmallIntWriter writer){
    fail("CopyAsFieldSmallInt");
  }

  public void read(IntHolder holder){
    fail("Int");
  }

  public void read(NullableIntHolder holder){
    fail("Int");
  }
  
  public void read(int arrayIndex, IntHolder holder){
    fail("RepeatedInt");
  }
  
  public void read(int arrayIndex, NullableIntHolder holder){
    fail("RepeatedInt");
  }
  
  public void copyAsValue(IntWriter writer){
    fail("CopyAsValueInt");
  }
  public void copyAsField(String name, IntWriter writer){
    fail("CopyAsFieldInt");
  }

  public void read(UInt4Holder holder){
    fail("UInt4");
  }

  public void read(NullableUInt4Holder holder){
    fail("UInt4");
  }
  
  public void read(int arrayIndex, UInt4Holder holder){
    fail("RepeatedUInt4");
  }
  
  public void read(int arrayIndex, NullableUInt4Holder holder){
    fail("RepeatedUInt4");
  }
  
  public void copyAsValue(UInt4Writer writer){
    fail("CopyAsValueUInt4");
  }
  public void copyAsField(String name, UInt4Writer writer){
    fail("CopyAsFieldUInt4");
  }

  public void read(Float4Holder holder){
    fail("Float4");
  }

  public void read(NullableFloat4Holder holder){
    fail("Float4");
  }
  
  public void read(int arrayIndex, Float4Holder holder){
    fail("RepeatedFloat4");
  }
  
  public void read(int arrayIndex, NullableFloat4Holder holder){
    fail("RepeatedFloat4");
  }
  
  public void copyAsValue(Float4Writer writer){
    fail("CopyAsValueFloat4");
  }
  public void copyAsField(String name, Float4Writer writer){
    fail("CopyAsFieldFloat4");
  }

  public void read(TimeHolder holder){
    fail("Time");
  }

  public void read(NullableTimeHolder holder){
    fail("Time");
  }
  
  public void read(int arrayIndex, TimeHolder holder){
    fail("RepeatedTime");
  }
  
  public void read(int arrayIndex, NullableTimeHolder holder){
    fail("RepeatedTime");
  }
  
  public void copyAsValue(TimeWriter writer){
    fail("CopyAsValueTime");
  }
  public void copyAsField(String name, TimeWriter writer){
    fail("CopyAsFieldTime");
  }

  public void read(IntervalYearHolder holder){
    fail("IntervalYear");
  }

  public void read(NullableIntervalYearHolder holder){
    fail("IntervalYear");
  }
  
  public void read(int arrayIndex, IntervalYearHolder holder){
    fail("RepeatedIntervalYear");
  }
  
  public void read(int arrayIndex, NullableIntervalYearHolder holder){
    fail("RepeatedIntervalYear");
  }
  
  public void copyAsValue(IntervalYearWriter writer){
    fail("CopyAsValueIntervalYear");
  }
  public void copyAsField(String name, IntervalYearWriter writer){
    fail("CopyAsFieldIntervalYear");
  }

  public void read(Decimal9Holder holder){
    fail("Decimal9");
  }

  public void read(NullableDecimal9Holder holder){
    fail("Decimal9");
  }
  
  public void read(int arrayIndex, Decimal9Holder holder){
    fail("RepeatedDecimal9");
  }
  
  public void read(int arrayIndex, NullableDecimal9Holder holder){
    fail("RepeatedDecimal9");
  }
  
  public void copyAsValue(Decimal9Writer writer){
    fail("CopyAsValueDecimal9");
  }
  public void copyAsField(String name, Decimal9Writer writer){
    fail("CopyAsFieldDecimal9");
  }

  public void read(BigIntHolder holder){
    fail("BigInt");
  }

  public void read(NullableBigIntHolder holder){
    fail("BigInt");
  }
  
  public void read(int arrayIndex, BigIntHolder holder){
    fail("RepeatedBigInt");
  }
  
  public void read(int arrayIndex, NullableBigIntHolder holder){
    fail("RepeatedBigInt");
  }
  
  public void copyAsValue(BigIntWriter writer){
    fail("CopyAsValueBigInt");
  }
  public void copyAsField(String name, BigIntWriter writer){
    fail("CopyAsFieldBigInt");
  }

  public void read(UInt8Holder holder){
    fail("UInt8");
  }

  public void read(NullableUInt8Holder holder){
    fail("UInt8");
  }
  
  public void read(int arrayIndex, UInt8Holder holder){
    fail("RepeatedUInt8");
  }
  
  public void read(int arrayIndex, NullableUInt8Holder holder){
    fail("RepeatedUInt8");
  }
  
  public void copyAsValue(UInt8Writer writer){
    fail("CopyAsValueUInt8");
  }
  public void copyAsField(String name, UInt8Writer writer){
    fail("CopyAsFieldUInt8");
  }

  public void read(Float8Holder holder){
    fail("Float8");
  }

  public void read(NullableFloat8Holder holder){
    fail("Float8");
  }
  
  public void read(int arrayIndex, Float8Holder holder){
    fail("RepeatedFloat8");
  }
  
  public void read(int arrayIndex, NullableFloat8Holder holder){
    fail("RepeatedFloat8");
  }
  
  public void copyAsValue(Float8Writer writer){
    fail("CopyAsValueFloat8");
  }
  public void copyAsField(String name, Float8Writer writer){
    fail("CopyAsFieldFloat8");
  }

  public void read(DateHolder holder){
    fail("Date");
  }

  public void read(NullableDateHolder holder){
    fail("Date");
  }
  
  public void read(int arrayIndex, DateHolder holder){
    fail("RepeatedDate");
  }
  
  public void read(int arrayIndex, NullableDateHolder holder){
    fail("RepeatedDate");
  }
  
  public void copyAsValue(DateWriter writer){
    fail("CopyAsValueDate");
  }
  public void copyAsField(String name, DateWriter writer){
    fail("CopyAsFieldDate");
  }

  public void read(TimeStampHolder holder){
    fail("TimeStamp");
  }

  public void read(NullableTimeStampHolder holder){
    fail("TimeStamp");
  }
  
  public void read(int arrayIndex, TimeStampHolder holder){
    fail("RepeatedTimeStamp");
  }
  
  public void read(int arrayIndex, NullableTimeStampHolder holder){
    fail("RepeatedTimeStamp");
  }
  
  public void copyAsValue(TimeStampWriter writer){
    fail("CopyAsValueTimeStamp");
  }
  public void copyAsField(String name, TimeStampWriter writer){
    fail("CopyAsFieldTimeStamp");
  }

  public void read(Decimal18Holder holder){
    fail("Decimal18");
  }

  public void read(NullableDecimal18Holder holder){
    fail("Decimal18");
  }
  
  public void read(int arrayIndex, Decimal18Holder holder){
    fail("RepeatedDecimal18");
  }
  
  public void read(int arrayIndex, NullableDecimal18Holder holder){
    fail("RepeatedDecimal18");
  }
  
  public void copyAsValue(Decimal18Writer writer){
    fail("CopyAsValueDecimal18");
  }
  public void copyAsField(String name, Decimal18Writer writer){
    fail("CopyAsFieldDecimal18");
  }

  public void read(IntervalDayHolder holder){
    fail("IntervalDay");
  }

  public void read(NullableIntervalDayHolder holder){
    fail("IntervalDay");
  }
  
  public void read(int arrayIndex, IntervalDayHolder holder){
    fail("RepeatedIntervalDay");
  }
  
  public void read(int arrayIndex, NullableIntervalDayHolder holder){
    fail("RepeatedIntervalDay");
  }
  
  public void copyAsValue(IntervalDayWriter writer){
    fail("CopyAsValueIntervalDay");
  }
  public void copyAsField(String name, IntervalDayWriter writer){
    fail("CopyAsFieldIntervalDay");
  }

  public void read(IntervalHolder holder){
    fail("Interval");
  }

  public void read(NullableIntervalHolder holder){
    fail("Interval");
  }
  
  public void read(int arrayIndex, IntervalHolder holder){
    fail("RepeatedInterval");
  }
  
  public void read(int arrayIndex, NullableIntervalHolder holder){
    fail("RepeatedInterval");
  }
  
  public void copyAsValue(IntervalWriter writer){
    fail("CopyAsValueInterval");
  }
  public void copyAsField(String name, IntervalWriter writer){
    fail("CopyAsFieldInterval");
  }

  public void read(Decimal28DenseHolder holder){
    fail("Decimal28Dense");
  }

  public void read(NullableDecimal28DenseHolder holder){
    fail("Decimal28Dense");
  }
  
  public void read(int arrayIndex, Decimal28DenseHolder holder){
    fail("RepeatedDecimal28Dense");
  }
  
  public void read(int arrayIndex, NullableDecimal28DenseHolder holder){
    fail("RepeatedDecimal28Dense");
  }
  
  public void copyAsValue(Decimal28DenseWriter writer){
    fail("CopyAsValueDecimal28Dense");
  }
  public void copyAsField(String name, Decimal28DenseWriter writer){
    fail("CopyAsFieldDecimal28Dense");
  }

  public void read(Decimal38DenseHolder holder){
    fail("Decimal38Dense");
  }

  public void read(NullableDecimal38DenseHolder holder){
    fail("Decimal38Dense");
  }
  
  public void read(int arrayIndex, Decimal38DenseHolder holder){
    fail("RepeatedDecimal38Dense");
  }
  
  public void read(int arrayIndex, NullableDecimal38DenseHolder holder){
    fail("RepeatedDecimal38Dense");
  }
  
  public void copyAsValue(Decimal38DenseWriter writer){
    fail("CopyAsValueDecimal38Dense");
  }
  public void copyAsField(String name, Decimal38DenseWriter writer){
    fail("CopyAsFieldDecimal38Dense");
  }

  public void read(Decimal38SparseHolder holder){
    fail("Decimal38Sparse");
  }

  public void read(NullableDecimal38SparseHolder holder){
    fail("Decimal38Sparse");
  }
  
  public void read(int arrayIndex, Decimal38SparseHolder holder){
    fail("RepeatedDecimal38Sparse");
  }
  
  public void read(int arrayIndex, NullableDecimal38SparseHolder holder){
    fail("RepeatedDecimal38Sparse");
  }
  
  public void copyAsValue(Decimal38SparseWriter writer){
    fail("CopyAsValueDecimal38Sparse");
  }
  public void copyAsField(String name, Decimal38SparseWriter writer){
    fail("CopyAsFieldDecimal38Sparse");
  }

  public void read(Decimal28SparseHolder holder){
    fail("Decimal28Sparse");
  }

  public void read(NullableDecimal28SparseHolder holder){
    fail("Decimal28Sparse");
  }
  
  public void read(int arrayIndex, Decimal28SparseHolder holder){
    fail("RepeatedDecimal28Sparse");
  }
  
  public void read(int arrayIndex, NullableDecimal28SparseHolder holder){
    fail("RepeatedDecimal28Sparse");
  }
  
  public void copyAsValue(Decimal28SparseWriter writer){
    fail("CopyAsValueDecimal28Sparse");
  }
  public void copyAsField(String name, Decimal28SparseWriter writer){
    fail("CopyAsFieldDecimal28Sparse");
  }

  public void read(VarBinaryHolder holder){
    fail("VarBinary");
  }

  public void read(NullableVarBinaryHolder holder){
    fail("VarBinary");
  }
  
  public void read(int arrayIndex, VarBinaryHolder holder){
    fail("RepeatedVarBinary");
  }
  
  public void read(int arrayIndex, NullableVarBinaryHolder holder){
    fail("RepeatedVarBinary");
  }
  
  public void copyAsValue(VarBinaryWriter writer){
    fail("CopyAsValueVarBinary");
  }
  public void copyAsField(String name, VarBinaryWriter writer){
    fail("CopyAsFieldVarBinary");
  }

  public void read(VarCharHolder holder){
    fail("VarChar");
  }

  public void read(NullableVarCharHolder holder){
    fail("VarChar");
  }
  
  public void read(int arrayIndex, VarCharHolder holder){
    fail("RepeatedVarChar");
  }
  
  public void read(int arrayIndex, NullableVarCharHolder holder){
    fail("RepeatedVarChar");
  }
  
  public void copyAsValue(VarCharWriter writer){
    fail("CopyAsValueVarChar");
  }
  public void copyAsField(String name, VarCharWriter writer){
    fail("CopyAsFieldVarChar");
  }

  public void read(Var16CharHolder holder){
    fail("Var16Char");
  }

  public void read(NullableVar16CharHolder holder){
    fail("Var16Char");
  }
  
  public void read(int arrayIndex, Var16CharHolder holder){
    fail("RepeatedVar16Char");
  }
  
  public void read(int arrayIndex, NullableVar16CharHolder holder){
    fail("RepeatedVar16Char");
  }
  
  public void copyAsValue(Var16CharWriter writer){
    fail("CopyAsValueVar16Char");
  }
  public void copyAsField(String name, Var16CharWriter writer){
    fail("CopyAsFieldVar16Char");
  }

  public void read(BitHolder holder){
    fail("Bit");
  }

  public void read(NullableBitHolder holder){
    fail("Bit");
  }
  
  public void read(int arrayIndex, BitHolder holder){
    fail("RepeatedBit");
  }
  
  public void read(int arrayIndex, NullableBitHolder holder){
    fail("RepeatedBit");
  }
  
  public void copyAsValue(BitWriter writer){
    fail("CopyAsValueBit");
  }
  public void copyAsField(String name, BitWriter writer){
    fail("CopyAsFieldBit");
  }
  
  public FieldReader reader(String name){
    fail("reader(String name)");
    return null;
  }

  public FieldReader reader(){
    fail("reader()");
    return null;
    
  }
  
  public int size(){
    fail("size()");
    return -1;
  }
  
  private void fail(String name){
    throw new IllegalArgumentException(String.format("You tried to read a [%s] type when you are using a field reader of type [%s].", name, this.getClass().getSimpleName()));
  }
  
  
}



