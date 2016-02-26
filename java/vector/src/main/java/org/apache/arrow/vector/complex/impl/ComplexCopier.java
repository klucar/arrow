

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
 * This class is generated using freemarker and the ComplexCopier.java template.
 */
@SuppressWarnings("unused")
public class ComplexCopier {

  /**
   * Do a deep copy of the value in input into output
   * @param in
   * @param out
   */
  public static void copy(FieldReader input, FieldWriter output) {
    writeValue(input, output);
  }

  private static void writeValue(FieldReader reader, FieldWriter writer) {
    final DataMode m = reader.getType().getMode();
    final MinorType mt = reader.getType().getMinorType();

    switch(m){
    case OPTIONAL:
    case REQUIRED:


      switch (mt) {

      case LIST:
        writer.startList();
        while (reader.next()) {
          writeValue(reader.reader(), getListWriterForReader(reader.reader(), writer));
        }
        writer.endList();
        break;
      case MAP:
        writer.start();
        if (reader.isSet()) {
          for(String name : reader){
            FieldReader childReader = reader.reader(name);
            if(childReader.isSet()){
              writeValue(childReader, getMapWriterForReader(childReader, writer, name));
            }
          }
        }
        writer.end();
        break;

      case TINYINT:
        if (reader.isSet()) {
          NullableTinyIntHolder tinyIntHolder = new NullableTinyIntHolder();
          reader.read(tinyIntHolder);
          if (tinyIntHolder.isSet == 1) {
            writer.writeTinyInt(tinyIntHolder.value);
          }
        }
        break;


      case UINT1:
        if (reader.isSet()) {
          NullableUInt1Holder uInt1Holder = new NullableUInt1Holder();
          reader.read(uInt1Holder);
          if (uInt1Holder.isSet == 1) {
            writer.writeUInt1(uInt1Holder.value);
          }
        }
        break;


      case UINT2:
        if (reader.isSet()) {
          NullableUInt2Holder uInt2Holder = new NullableUInt2Holder();
          reader.read(uInt2Holder);
          if (uInt2Holder.isSet == 1) {
            writer.writeUInt2(uInt2Holder.value);
          }
        }
        break;


      case SMALLINT:
        if (reader.isSet()) {
          NullableSmallIntHolder smallIntHolder = new NullableSmallIntHolder();
          reader.read(smallIntHolder);
          if (smallIntHolder.isSet == 1) {
            writer.writeSmallInt(smallIntHolder.value);
          }
        }
        break;


      case INT:
        if (reader.isSet()) {
          NullableIntHolder intHolder = new NullableIntHolder();
          reader.read(intHolder);
          if (intHolder.isSet == 1) {
            writer.writeInt(intHolder.value);
          }
        }
        break;


      case UINT4:
        if (reader.isSet()) {
          NullableUInt4Holder uInt4Holder = new NullableUInt4Holder();
          reader.read(uInt4Holder);
          if (uInt4Holder.isSet == 1) {
            writer.writeUInt4(uInt4Holder.value);
          }
        }
        break;


      case FLOAT4:
        if (reader.isSet()) {
          NullableFloat4Holder float4Holder = new NullableFloat4Holder();
          reader.read(float4Holder);
          if (float4Holder.isSet == 1) {
            writer.writeFloat4(float4Holder.value);
          }
        }
        break;


      case TIME:
        if (reader.isSet()) {
          NullableTimeHolder timeHolder = new NullableTimeHolder();
          reader.read(timeHolder);
          if (timeHolder.isSet == 1) {
            writer.writeTime(timeHolder.value);
          }
        }
        break;


      case INTERVALYEAR:
        if (reader.isSet()) {
          NullableIntervalYearHolder intervalYearHolder = new NullableIntervalYearHolder();
          reader.read(intervalYearHolder);
          if (intervalYearHolder.isSet == 1) {
            writer.writeIntervalYear(intervalYearHolder.value);
          }
        }
        break;


      case BIGINT:
        if (reader.isSet()) {
          NullableBigIntHolder bigIntHolder = new NullableBigIntHolder();
          reader.read(bigIntHolder);
          if (bigIntHolder.isSet == 1) {
            writer.writeBigInt(bigIntHolder.value);
          }
        }
        break;


      case UINT8:
        if (reader.isSet()) {
          NullableUInt8Holder uInt8Holder = new NullableUInt8Holder();
          reader.read(uInt8Holder);
          if (uInt8Holder.isSet == 1) {
            writer.writeUInt8(uInt8Holder.value);
          }
        }
        break;


      case FLOAT8:
        if (reader.isSet()) {
          NullableFloat8Holder float8Holder = new NullableFloat8Holder();
          reader.read(float8Holder);
          if (float8Holder.isSet == 1) {
            writer.writeFloat8(float8Holder.value);
          }
        }
        break;


      case DATE:
        if (reader.isSet()) {
          NullableDateHolder dateHolder = new NullableDateHolder();
          reader.read(dateHolder);
          if (dateHolder.isSet == 1) {
            writer.writeDate(dateHolder.value);
          }
        }
        break;


      case TIMESTAMP:
        if (reader.isSet()) {
          NullableTimeStampHolder timeStampHolder = new NullableTimeStampHolder();
          reader.read(timeStampHolder);
          if (timeStampHolder.isSet == 1) {
            writer.writeTimeStamp(timeStampHolder.value);
          }
        }
        break;


      case INTERVALDAY:
        if (reader.isSet()) {
          NullableIntervalDayHolder intervalDayHolder = new NullableIntervalDayHolder();
          reader.read(intervalDayHolder);
          if (intervalDayHolder.isSet == 1) {
            writer.writeIntervalDay(intervalDayHolder.days, intervalDayHolder.milliseconds);
          }
        }
        break;


      case INTERVAL:
        if (reader.isSet()) {
          NullableIntervalHolder intervalHolder = new NullableIntervalHolder();
          reader.read(intervalHolder);
          if (intervalHolder.isSet == 1) {
            writer.writeInterval(intervalHolder.months, intervalHolder.days, intervalHolder.milliseconds);
          }
        }
        break;


      case VARBINARY:
        if (reader.isSet()) {
          NullableVarBinaryHolder varBinaryHolder = new NullableVarBinaryHolder();
          reader.read(varBinaryHolder);
          if (varBinaryHolder.isSet == 1) {
            writer.writeVarBinary(varBinaryHolder.start, varBinaryHolder.end, varBinaryHolder.buffer);
          }
        }
        break;


      case VARCHAR:
        if (reader.isSet()) {
          NullableVarCharHolder varCharHolder = new NullableVarCharHolder();
          reader.read(varCharHolder);
          if (varCharHolder.isSet == 1) {
            writer.writeVarChar(varCharHolder.start, varCharHolder.end, varCharHolder.buffer);
          }
        }
        break;


      case VAR16CHAR:
        if (reader.isSet()) {
          NullableVar16CharHolder var16CharHolder = new NullableVar16CharHolder();
          reader.read(var16CharHolder);
          if (var16CharHolder.isSet == 1) {
            writer.writeVar16Char(var16CharHolder.start, var16CharHolder.end, var16CharHolder.buffer);
          }
        }
        break;


      case BIT:
        if (reader.isSet()) {
          NullableBitHolder bitHolder = new NullableBitHolder();
          reader.read(bitHolder);
          if (bitHolder.isSet == 1) {
            writer.writeBit(bitHolder.value);
          }
        }
        break;

      }
              break;
    }
 }

  private static FieldWriter getMapWriterForReader(FieldReader reader, MapWriter writer, String name) {
    switch (reader.getType().getMinorType()) {
    case TINYINT:
      return (FieldWriter) writer.tinyInt(name);
    case UINT1:
      return (FieldWriter) writer.uInt1(name);
    case UINT2:
      return (FieldWriter) writer.uInt2(name);
    case SMALLINT:
      return (FieldWriter) writer.smallInt(name);
    case INT:
      return (FieldWriter) writer.integer(name);
    case UINT4:
      return (FieldWriter) writer.uInt4(name);
    case FLOAT4:
      return (FieldWriter) writer.float4(name);
    case TIME:
      return (FieldWriter) writer.time(name);
    case INTERVALYEAR:
      return (FieldWriter) writer.intervalYear(name);
    case BIGINT:
      return (FieldWriter) writer.bigInt(name);
    case UINT8:
      return (FieldWriter) writer.uInt8(name);
    case FLOAT8:
      return (FieldWriter) writer.float8(name);
    case DATE:
      return (FieldWriter) writer.date(name);
    case TIMESTAMP:
      return (FieldWriter) writer.timeStamp(name);
    case INTERVALDAY:
      return (FieldWriter) writer.intervalDay(name);
    case INTERVAL:
      return (FieldWriter) writer.interval(name);
    case VARBINARY:
      return (FieldWriter) writer.varBinary(name);
    case VARCHAR:
      return (FieldWriter) writer.varChar(name);
    case VAR16CHAR:
      return (FieldWriter) writer.var16Char(name);
    case BIT:
      return (FieldWriter) writer.bit(name);
    case MAP:
      return (FieldWriter) writer.map(name);
    case LIST:
      return (FieldWriter) writer.list(name);
    default:
      throw new UnsupportedOperationException(reader.getType().toString());
    }
  }

  private static FieldWriter getListWriterForReader(FieldReader reader, ListWriter writer) {
    switch (reader.getType().getMinorType()) {
    case TINYINT:
    return (FieldWriter) writer.tinyInt();
    case UINT1:
    return (FieldWriter) writer.uInt1();
    case UINT2:
    return (FieldWriter) writer.uInt2();
    case SMALLINT:
    return (FieldWriter) writer.smallInt();
    case INT:
    return (FieldWriter) writer.integer();
    case UINT4:
    return (FieldWriter) writer.uInt4();
    case FLOAT4:
    return (FieldWriter) writer.float4();
    case TIME:
    return (FieldWriter) writer.time();
    case INTERVALYEAR:
    return (FieldWriter) writer.intervalYear();
    case BIGINT:
    return (FieldWriter) writer.bigInt();
    case UINT8:
    return (FieldWriter) writer.uInt8();
    case FLOAT8:
    return (FieldWriter) writer.float8();
    case DATE:
    return (FieldWriter) writer.date();
    case TIMESTAMP:
    return (FieldWriter) writer.timeStamp();
    case INTERVALDAY:
    return (FieldWriter) writer.intervalDay();
    case INTERVAL:
    return (FieldWriter) writer.interval();
    case VARBINARY:
    return (FieldWriter) writer.varBinary();
    case VARCHAR:
    return (FieldWriter) writer.varChar();
    case VAR16CHAR:
    return (FieldWriter) writer.var16Char();
    case BIT:
    return (FieldWriter) writer.bit();
    case MAP:
      return (FieldWriter) writer.map();
    case LIST:
      return (FieldWriter) writer.list();
    default:
      throw new UnsupportedOperationException(reader.getType().toString());
    }
  }
}
