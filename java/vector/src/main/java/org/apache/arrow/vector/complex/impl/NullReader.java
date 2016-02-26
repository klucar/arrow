

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
public class NullReader extends AbstractBaseReader implements FieldReader{
  
  public static final NullReader INSTANCE = new NullReader();
  public static final NullReader EMPTY_LIST_INSTANCE = new NullReader(Types.repeated(MinorType.NULL));
  public static final NullReader EMPTY_MAP_INSTANCE = new NullReader(Types.required(MinorType.MAP));
  private MajorType type;
  
  private NullReader(){
    super();
    type = Types.required(MinorType.NULL);
  }

  private NullReader(MajorType type){
    super();
    this.type = type;
  }

  @Override
  public MajorType getType() {
    return type;
  }
  
  public void copyAsValue(MapWriter writer) {}

  public void copyAsValue(ListWriter writer) {}

  public void copyAsValue(UnionWriter writer) {}

  public void read(TinyIntHolder holder){
    throw new UnsupportedOperationException("NullReader cannot write into non-nullable holder");
  }

  public void read(NullableTinyIntHolder holder){
    holder.isSet = 0;
  }

  public void read(int arrayIndex, TinyIntHolder holder){
    throw new ArrayIndexOutOfBoundsException();
  }
  
  public void copyAsValue(TinyIntWriter writer){}
  public void copyAsField(String name, TinyIntWriter writer){}

  public void read(int arrayIndex, NullableTinyIntHolder holder){
    throw new ArrayIndexOutOfBoundsException();
  }
  public void read(UInt1Holder holder){
    throw new UnsupportedOperationException("NullReader cannot write into non-nullable holder");
  }

  public void read(NullableUInt1Holder holder){
    holder.isSet = 0;
  }

  public void read(int arrayIndex, UInt1Holder holder){
    throw new ArrayIndexOutOfBoundsException();
  }
  
  public void copyAsValue(UInt1Writer writer){}
  public void copyAsField(String name, UInt1Writer writer){}

  public void read(int arrayIndex, NullableUInt1Holder holder){
    throw new ArrayIndexOutOfBoundsException();
  }
  public void read(UInt2Holder holder){
    throw new UnsupportedOperationException("NullReader cannot write into non-nullable holder");
  }

  public void read(NullableUInt2Holder holder){
    holder.isSet = 0;
  }

  public void read(int arrayIndex, UInt2Holder holder){
    throw new ArrayIndexOutOfBoundsException();
  }
  
  public void copyAsValue(UInt2Writer writer){}
  public void copyAsField(String name, UInt2Writer writer){}

  public void read(int arrayIndex, NullableUInt2Holder holder){
    throw new ArrayIndexOutOfBoundsException();
  }
  public void read(SmallIntHolder holder){
    throw new UnsupportedOperationException("NullReader cannot write into non-nullable holder");
  }

  public void read(NullableSmallIntHolder holder){
    holder.isSet = 0;
  }

  public void read(int arrayIndex, SmallIntHolder holder){
    throw new ArrayIndexOutOfBoundsException();
  }
  
  public void copyAsValue(SmallIntWriter writer){}
  public void copyAsField(String name, SmallIntWriter writer){}

  public void read(int arrayIndex, NullableSmallIntHolder holder){
    throw new ArrayIndexOutOfBoundsException();
  }
  public void read(IntHolder holder){
    throw new UnsupportedOperationException("NullReader cannot write into non-nullable holder");
  }

  public void read(NullableIntHolder holder){
    holder.isSet = 0;
  }

  public void read(int arrayIndex, IntHolder holder){
    throw new ArrayIndexOutOfBoundsException();
  }
  
  public void copyAsValue(IntWriter writer){}
  public void copyAsField(String name, IntWriter writer){}

  public void read(int arrayIndex, NullableIntHolder holder){
    throw new ArrayIndexOutOfBoundsException();
  }
  public void read(UInt4Holder holder){
    throw new UnsupportedOperationException("NullReader cannot write into non-nullable holder");
  }

  public void read(NullableUInt4Holder holder){
    holder.isSet = 0;
  }

  public void read(int arrayIndex, UInt4Holder holder){
    throw new ArrayIndexOutOfBoundsException();
  }
  
  public void copyAsValue(UInt4Writer writer){}
  public void copyAsField(String name, UInt4Writer writer){}

  public void read(int arrayIndex, NullableUInt4Holder holder){
    throw new ArrayIndexOutOfBoundsException();
  }
  public void read(Float4Holder holder){
    throw new UnsupportedOperationException("NullReader cannot write into non-nullable holder");
  }

  public void read(NullableFloat4Holder holder){
    holder.isSet = 0;
  }

  public void read(int arrayIndex, Float4Holder holder){
    throw new ArrayIndexOutOfBoundsException();
  }
  
  public void copyAsValue(Float4Writer writer){}
  public void copyAsField(String name, Float4Writer writer){}

  public void read(int arrayIndex, NullableFloat4Holder holder){
    throw new ArrayIndexOutOfBoundsException();
  }
  public void read(TimeHolder holder){
    throw new UnsupportedOperationException("NullReader cannot write into non-nullable holder");
  }

  public void read(NullableTimeHolder holder){
    holder.isSet = 0;
  }

  public void read(int arrayIndex, TimeHolder holder){
    throw new ArrayIndexOutOfBoundsException();
  }
  
  public void copyAsValue(TimeWriter writer){}
  public void copyAsField(String name, TimeWriter writer){}

  public void read(int arrayIndex, NullableTimeHolder holder){
    throw new ArrayIndexOutOfBoundsException();
  }
  public void read(IntervalYearHolder holder){
    throw new UnsupportedOperationException("NullReader cannot write into non-nullable holder");
  }

  public void read(NullableIntervalYearHolder holder){
    holder.isSet = 0;
  }

  public void read(int arrayIndex, IntervalYearHolder holder){
    throw new ArrayIndexOutOfBoundsException();
  }
  
  public void copyAsValue(IntervalYearWriter writer){}
  public void copyAsField(String name, IntervalYearWriter writer){}

  public void read(int arrayIndex, NullableIntervalYearHolder holder){
    throw new ArrayIndexOutOfBoundsException();
  }
  public void read(Decimal9Holder holder){
    throw new UnsupportedOperationException("NullReader cannot write into non-nullable holder");
  }

  public void read(NullableDecimal9Holder holder){
    holder.isSet = 0;
  }

  public void read(int arrayIndex, Decimal9Holder holder){
    throw new ArrayIndexOutOfBoundsException();
  }
  
  public void copyAsValue(Decimal9Writer writer){}
  public void copyAsField(String name, Decimal9Writer writer){}

  public void read(int arrayIndex, NullableDecimal9Holder holder){
    throw new ArrayIndexOutOfBoundsException();
  }
  public void read(BigIntHolder holder){
    throw new UnsupportedOperationException("NullReader cannot write into non-nullable holder");
  }

  public void read(NullableBigIntHolder holder){
    holder.isSet = 0;
  }

  public void read(int arrayIndex, BigIntHolder holder){
    throw new ArrayIndexOutOfBoundsException();
  }
  
  public void copyAsValue(BigIntWriter writer){}
  public void copyAsField(String name, BigIntWriter writer){}

  public void read(int arrayIndex, NullableBigIntHolder holder){
    throw new ArrayIndexOutOfBoundsException();
  }
  public void read(UInt8Holder holder){
    throw new UnsupportedOperationException("NullReader cannot write into non-nullable holder");
  }

  public void read(NullableUInt8Holder holder){
    holder.isSet = 0;
  }

  public void read(int arrayIndex, UInt8Holder holder){
    throw new ArrayIndexOutOfBoundsException();
  }
  
  public void copyAsValue(UInt8Writer writer){}
  public void copyAsField(String name, UInt8Writer writer){}

  public void read(int arrayIndex, NullableUInt8Holder holder){
    throw new ArrayIndexOutOfBoundsException();
  }
  public void read(Float8Holder holder){
    throw new UnsupportedOperationException("NullReader cannot write into non-nullable holder");
  }

  public void read(NullableFloat8Holder holder){
    holder.isSet = 0;
  }

  public void read(int arrayIndex, Float8Holder holder){
    throw new ArrayIndexOutOfBoundsException();
  }
  
  public void copyAsValue(Float8Writer writer){}
  public void copyAsField(String name, Float8Writer writer){}

  public void read(int arrayIndex, NullableFloat8Holder holder){
    throw new ArrayIndexOutOfBoundsException();
  }
  public void read(DateHolder holder){
    throw new UnsupportedOperationException("NullReader cannot write into non-nullable holder");
  }

  public void read(NullableDateHolder holder){
    holder.isSet = 0;
  }

  public void read(int arrayIndex, DateHolder holder){
    throw new ArrayIndexOutOfBoundsException();
  }
  
  public void copyAsValue(DateWriter writer){}
  public void copyAsField(String name, DateWriter writer){}

  public void read(int arrayIndex, NullableDateHolder holder){
    throw new ArrayIndexOutOfBoundsException();
  }
  public void read(TimeStampHolder holder){
    throw new UnsupportedOperationException("NullReader cannot write into non-nullable holder");
  }

  public void read(NullableTimeStampHolder holder){
    holder.isSet = 0;
  }

  public void read(int arrayIndex, TimeStampHolder holder){
    throw new ArrayIndexOutOfBoundsException();
  }
  
  public void copyAsValue(TimeStampWriter writer){}
  public void copyAsField(String name, TimeStampWriter writer){}

  public void read(int arrayIndex, NullableTimeStampHolder holder){
    throw new ArrayIndexOutOfBoundsException();
  }
  public void read(Decimal18Holder holder){
    throw new UnsupportedOperationException("NullReader cannot write into non-nullable holder");
  }

  public void read(NullableDecimal18Holder holder){
    holder.isSet = 0;
  }

  public void read(int arrayIndex, Decimal18Holder holder){
    throw new ArrayIndexOutOfBoundsException();
  }
  
  public void copyAsValue(Decimal18Writer writer){}
  public void copyAsField(String name, Decimal18Writer writer){}

  public void read(int arrayIndex, NullableDecimal18Holder holder){
    throw new ArrayIndexOutOfBoundsException();
  }
  public void read(IntervalDayHolder holder){
    throw new UnsupportedOperationException("NullReader cannot write into non-nullable holder");
  }

  public void read(NullableIntervalDayHolder holder){
    holder.isSet = 0;
  }

  public void read(int arrayIndex, IntervalDayHolder holder){
    throw new ArrayIndexOutOfBoundsException();
  }
  
  public void copyAsValue(IntervalDayWriter writer){}
  public void copyAsField(String name, IntervalDayWriter writer){}

  public void read(int arrayIndex, NullableIntervalDayHolder holder){
    throw new ArrayIndexOutOfBoundsException();
  }
  public void read(IntervalHolder holder){
    throw new UnsupportedOperationException("NullReader cannot write into non-nullable holder");
  }

  public void read(NullableIntervalHolder holder){
    holder.isSet = 0;
  }

  public void read(int arrayIndex, IntervalHolder holder){
    throw new ArrayIndexOutOfBoundsException();
  }
  
  public void copyAsValue(IntervalWriter writer){}
  public void copyAsField(String name, IntervalWriter writer){}

  public void read(int arrayIndex, NullableIntervalHolder holder){
    throw new ArrayIndexOutOfBoundsException();
  }
  public void read(Decimal28DenseHolder holder){
    throw new UnsupportedOperationException("NullReader cannot write into non-nullable holder");
  }

  public void read(NullableDecimal28DenseHolder holder){
    holder.isSet = 0;
  }

  public void read(int arrayIndex, Decimal28DenseHolder holder){
    throw new ArrayIndexOutOfBoundsException();
  }
  
  public void copyAsValue(Decimal28DenseWriter writer){}
  public void copyAsField(String name, Decimal28DenseWriter writer){}

  public void read(int arrayIndex, NullableDecimal28DenseHolder holder){
    throw new ArrayIndexOutOfBoundsException();
  }
  public void read(Decimal38DenseHolder holder){
    throw new UnsupportedOperationException("NullReader cannot write into non-nullable holder");
  }

  public void read(NullableDecimal38DenseHolder holder){
    holder.isSet = 0;
  }

  public void read(int arrayIndex, Decimal38DenseHolder holder){
    throw new ArrayIndexOutOfBoundsException();
  }
  
  public void copyAsValue(Decimal38DenseWriter writer){}
  public void copyAsField(String name, Decimal38DenseWriter writer){}

  public void read(int arrayIndex, NullableDecimal38DenseHolder holder){
    throw new ArrayIndexOutOfBoundsException();
  }
  public void read(Decimal38SparseHolder holder){
    throw new UnsupportedOperationException("NullReader cannot write into non-nullable holder");
  }

  public void read(NullableDecimal38SparseHolder holder){
    holder.isSet = 0;
  }

  public void read(int arrayIndex, Decimal38SparseHolder holder){
    throw new ArrayIndexOutOfBoundsException();
  }
  
  public void copyAsValue(Decimal38SparseWriter writer){}
  public void copyAsField(String name, Decimal38SparseWriter writer){}

  public void read(int arrayIndex, NullableDecimal38SparseHolder holder){
    throw new ArrayIndexOutOfBoundsException();
  }
  public void read(Decimal28SparseHolder holder){
    throw new UnsupportedOperationException("NullReader cannot write into non-nullable holder");
  }

  public void read(NullableDecimal28SparseHolder holder){
    holder.isSet = 0;
  }

  public void read(int arrayIndex, Decimal28SparseHolder holder){
    throw new ArrayIndexOutOfBoundsException();
  }
  
  public void copyAsValue(Decimal28SparseWriter writer){}
  public void copyAsField(String name, Decimal28SparseWriter writer){}

  public void read(int arrayIndex, NullableDecimal28SparseHolder holder){
    throw new ArrayIndexOutOfBoundsException();
  }
  public void read(VarBinaryHolder holder){
    throw new UnsupportedOperationException("NullReader cannot write into non-nullable holder");
  }

  public void read(NullableVarBinaryHolder holder){
    holder.isSet = 0;
  }

  public void read(int arrayIndex, VarBinaryHolder holder){
    throw new ArrayIndexOutOfBoundsException();
  }
  
  public void copyAsValue(VarBinaryWriter writer){}
  public void copyAsField(String name, VarBinaryWriter writer){}

  public void read(int arrayIndex, NullableVarBinaryHolder holder){
    throw new ArrayIndexOutOfBoundsException();
  }
  public void read(VarCharHolder holder){
    throw new UnsupportedOperationException("NullReader cannot write into non-nullable holder");
  }

  public void read(NullableVarCharHolder holder){
    holder.isSet = 0;
  }

  public void read(int arrayIndex, VarCharHolder holder){
    throw new ArrayIndexOutOfBoundsException();
  }
  
  public void copyAsValue(VarCharWriter writer){}
  public void copyAsField(String name, VarCharWriter writer){}

  public void read(int arrayIndex, NullableVarCharHolder holder){
    throw new ArrayIndexOutOfBoundsException();
  }
  public void read(Var16CharHolder holder){
    throw new UnsupportedOperationException("NullReader cannot write into non-nullable holder");
  }

  public void read(NullableVar16CharHolder holder){
    holder.isSet = 0;
  }

  public void read(int arrayIndex, Var16CharHolder holder){
    throw new ArrayIndexOutOfBoundsException();
  }
  
  public void copyAsValue(Var16CharWriter writer){}
  public void copyAsField(String name, Var16CharWriter writer){}

  public void read(int arrayIndex, NullableVar16CharHolder holder){
    throw new ArrayIndexOutOfBoundsException();
  }
  public void read(BitHolder holder){
    throw new UnsupportedOperationException("NullReader cannot write into non-nullable holder");
  }

  public void read(NullableBitHolder holder){
    holder.isSet = 0;
  }

  public void read(int arrayIndex, BitHolder holder){
    throw new ArrayIndexOutOfBoundsException();
  }
  
  public void copyAsValue(BitWriter writer){}
  public void copyAsField(String name, BitWriter writer){}

  public void read(int arrayIndex, NullableBitHolder holder){
    throw new ArrayIndexOutOfBoundsException();
  }
  
  public int size(){
    return 0;
  }
  
  public boolean isSet(){
    return false;
  }
  
  public boolean next(){
    return false;
  }
  
  public RepeatedMapReader map(){
    return this;
  }
  
  public RepeatedListReader list(){
    return this;
  }
  
  public MapReader map(String name){
    return this;
  }
  
  public ListReader list(String name){
    return this;
  }
  
  public FieldReader reader(String name){
    return this;
  }
  
  public FieldReader reader(){
    return this;
  }
  
  private void fail(String name){
    throw new IllegalArgumentException(String.format("You tried to read a %s type when you are using a ValueReader of type %s.", name, this.getClass().getSimpleName()));
  }
  
  
  public Object readObject(int arrayIndex){
    return null;
  }
  
  public Object readObject(){
    return null;
  }
  
  public BigDecimal readBigDecimal(int arrayIndex){
    return null;
  }
  
  public BigDecimal readBigDecimal(){
    return null;
  }
  
  public Integer readInteger(int arrayIndex){
    return null;
  }
  
  public Integer readInteger(){
    return null;
  }
  
  public Long readLong(int arrayIndex){
    return null;
  }
  
  public Long readLong(){
    return null;
  }
  
  public Boolean readBoolean(int arrayIndex){
    return null;
  }
  
  public Boolean readBoolean(){
    return null;
  }
  
  public Character readCharacter(int arrayIndex){
    return null;
  }
  
  public Character readCharacter(){
    return null;
  }
  
  public DateTime readDateTime(int arrayIndex){
    return null;
  }
  
  public DateTime readDateTime(){
    return null;
  }
  
  public Period readPeriod(int arrayIndex){
    return null;
  }
  
  public Period readPeriod(){
    return null;
  }
  
  public Double readDouble(int arrayIndex){
    return null;
  }
  
  public Double readDouble(){
    return null;
  }
  
  public Float readFloat(int arrayIndex){
    return null;
  }
  
  public Float readFloat(){
    return null;
  }
  
  public Text readText(int arrayIndex){
    return null;
  }
  
  public Text readText(){
    return null;
  }
  
  public String readString(int arrayIndex){
    return null;
  }
  
  public String readString(){
    return null;
  }
  
  public Byte readByte(int arrayIndex){
    return null;
  }
  
  public Byte readByte(){
    return null;
  }
  
  public Short readShort(int arrayIndex){
    return null;
  }
  
  public Short readShort(){
    return null;
  }
  
  public byte[] readByteArray(int arrayIndex){
    return null;
  }
  
  public byte[] readByteArray(){
    return null;
  }
  
}



