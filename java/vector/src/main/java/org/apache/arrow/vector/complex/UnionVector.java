

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
package org.apache.arrow.vector.complex;


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






import java.util.ArrayList;
import java.util.Iterator;
import org.apache.arrow.vector.complex.impl.ComplexCopier;
import org.apache.arrow.vector.util.CallBack;
import org.apache.arrow.vector.util.BasicTypeHelper;

/*
 * This class is generated using freemarker and the UnionVector.java template.
 */
@SuppressWarnings("unused")


/**
 * A vector which can hold values of different types. It does so by using a MapVector which contains a vector for each
 * primitive type that is stored. MapVector is used in order to take advantage of its serialization/deserialization methods,
 * as well as the addOrGet method.
 *
 * For performance reasons, UnionVector stores a cached reference to each subtype vector, to avoid having to do the map lookup
 * each time the vector is accessed.
 */
public class UnionVector implements ValueVector {

  private MaterializedField field;
  private BufferAllocator allocator;
  private Accessor accessor = new Accessor();
  private Mutator mutator = new Mutator();
  private int valueCount;

  private MapVector internalMap;
  private UInt1Vector typeVector;

  private MapVector mapVector;
  private ListVector listVector;

  private FieldReader reader;
  private NullableBitVector bit;

  private int singleType = 0;
  private ValueVector singleVector;
  private MajorType majorType;

  private final CallBack callBack;

  public UnionVector(MaterializedField field, BufferAllocator allocator, CallBack callBack) {
    this.field = field.clone();
    this.allocator = allocator;
    this.internalMap = new MapVector("internal", allocator, callBack);
    this.typeVector = internalMap.addOrGet("types", new MajorType(MinorType.UINT1, DataMode.REQUIRED), UInt1Vector.class);
    this.field.addChild(internalMap.getField().clone());
    this.majorType = field.getType();
    this.callBack = callBack;
  }

  public BufferAllocator getAllocator() {
    return allocator;
  }

  public List<MinorType> getSubTypes() {
    return majorType.getSubTypes();
  }

  public void addSubType(MinorType type) {
    if (majorType.getSubTypes().contains(type)) {
      return;
    }
    List<MinorType> subTypes = this.majorType.getSubTypes();
    List<MinorType> newSubTypes = new ArrayList<>(subTypes);
    newSubTypes.add(type);
    majorType =  new MajorType(this.majorType.getMinorType(), this.majorType.getMode(), this.majorType.getPrecision(),
            this.majorType.getScale(), this.majorType.getTimezone(), newSubTypes);
    field = MaterializedField.create(field.getName(), majorType);
    if (callBack != null) {
      callBack.doWork();
    }
  }

  private static final MajorType MAP_TYPE = new MajorType(MinorType.MAP, DataMode.OPTIONAL);

  public MapVector getMap() {
    if (mapVector == null) {
      int vectorCount = internalMap.size();
      mapVector = internalMap.addOrGet("map", MAP_TYPE, MapVector.class);
      addSubType(MinorType.MAP);
      if (internalMap.size() > vectorCount) {
        mapVector.allocateNew();
      }
    }
    return mapVector;
  }


  private NullableTinyIntVector tinyIntVector;
  private static final MajorType TINYINT_TYPE = new MajorType(MinorType.TINYINT, DataMode.OPTIONAL);

  public NullableTinyIntVector getTinyIntVector() {
    if (tinyIntVector == null) {
      int vectorCount = internalMap.size();
      tinyIntVector = internalMap.addOrGet("tinyInt", TINYINT_TYPE, NullableTinyIntVector.class);
      addSubType(MinorType.TINYINT);
      if (internalMap.size() > vectorCount) {
        tinyIntVector.allocateNew();
      }
    }
    return tinyIntVector;
  }



  private NullableUInt1Vector uInt1Vector;
  private static final MajorType UINT1_TYPE = new MajorType(MinorType.UINT1, DataMode.OPTIONAL);

  public NullableUInt1Vector getUInt1Vector() {
    if (uInt1Vector == null) {
      int vectorCount = internalMap.size();
      uInt1Vector = internalMap.addOrGet("uInt1", UINT1_TYPE, NullableUInt1Vector.class);
      addSubType(MinorType.UINT1);
      if (internalMap.size() > vectorCount) {
        uInt1Vector.allocateNew();
      }
    }
    return uInt1Vector;
  }



  private NullableUInt2Vector uInt2Vector;
  private static final MajorType UINT2_TYPE = new MajorType(MinorType.UINT2, DataMode.OPTIONAL);

  public NullableUInt2Vector getUInt2Vector() {
    if (uInt2Vector == null) {
      int vectorCount = internalMap.size();
      uInt2Vector = internalMap.addOrGet("uInt2", UINT2_TYPE, NullableUInt2Vector.class);
      addSubType(MinorType.UINT2);
      if (internalMap.size() > vectorCount) {
        uInt2Vector.allocateNew();
      }
    }
    return uInt2Vector;
  }



  private NullableSmallIntVector smallIntVector;
  private static final MajorType SMALLINT_TYPE = new MajorType(MinorType.SMALLINT, DataMode.OPTIONAL);

  public NullableSmallIntVector getSmallIntVector() {
    if (smallIntVector == null) {
      int vectorCount = internalMap.size();
      smallIntVector = internalMap.addOrGet("smallInt", SMALLINT_TYPE, NullableSmallIntVector.class);
      addSubType(MinorType.SMALLINT);
      if (internalMap.size() > vectorCount) {
        smallIntVector.allocateNew();
      }
    }
    return smallIntVector;
  }



  private NullableIntVector intVector;
  private static final MajorType INT_TYPE = new MajorType(MinorType.INT, DataMode.OPTIONAL);

  public NullableIntVector getIntVector() {
    if (intVector == null) {
      int vectorCount = internalMap.size();
      intVector = internalMap.addOrGet("int", INT_TYPE, NullableIntVector.class);
      addSubType(MinorType.INT);
      if (internalMap.size() > vectorCount) {
        intVector.allocateNew();
      }
    }
    return intVector;
  }



  private NullableUInt4Vector uInt4Vector;
  private static final MajorType UINT4_TYPE = new MajorType(MinorType.UINT4, DataMode.OPTIONAL);

  public NullableUInt4Vector getUInt4Vector() {
    if (uInt4Vector == null) {
      int vectorCount = internalMap.size();
      uInt4Vector = internalMap.addOrGet("uInt4", UINT4_TYPE, NullableUInt4Vector.class);
      addSubType(MinorType.UINT4);
      if (internalMap.size() > vectorCount) {
        uInt4Vector.allocateNew();
      }
    }
    return uInt4Vector;
  }



  private NullableFloat4Vector float4Vector;
  private static final MajorType FLOAT4_TYPE = new MajorType(MinorType.FLOAT4, DataMode.OPTIONAL);

  public NullableFloat4Vector getFloat4Vector() {
    if (float4Vector == null) {
      int vectorCount = internalMap.size();
      float4Vector = internalMap.addOrGet("float4", FLOAT4_TYPE, NullableFloat4Vector.class);
      addSubType(MinorType.FLOAT4);
      if (internalMap.size() > vectorCount) {
        float4Vector.allocateNew();
      }
    }
    return float4Vector;
  }



  private NullableTimeVector timeVector;
  private static final MajorType TIME_TYPE = new MajorType(MinorType.TIME, DataMode.OPTIONAL);

  public NullableTimeVector getTimeVector() {
    if (timeVector == null) {
      int vectorCount = internalMap.size();
      timeVector = internalMap.addOrGet("time", TIME_TYPE, NullableTimeVector.class);
      addSubType(MinorType.TIME);
      if (internalMap.size() > vectorCount) {
        timeVector.allocateNew();
      }
    }
    return timeVector;
  }



  private NullableIntervalYearVector intervalYearVector;
  private static final MajorType INTERVALYEAR_TYPE = new MajorType(MinorType.INTERVALYEAR, DataMode.OPTIONAL);

  public NullableIntervalYearVector getIntervalYearVector() {
    if (intervalYearVector == null) {
      int vectorCount = internalMap.size();
      intervalYearVector = internalMap.addOrGet("intervalYear", INTERVALYEAR_TYPE, NullableIntervalYearVector.class);
      addSubType(MinorType.INTERVALYEAR);
      if (internalMap.size() > vectorCount) {
        intervalYearVector.allocateNew();
      }
    }
    return intervalYearVector;
  }




  private NullableBigIntVector bigIntVector;
  private static final MajorType BIGINT_TYPE = new MajorType(MinorType.BIGINT, DataMode.OPTIONAL);

  public NullableBigIntVector getBigIntVector() {
    if (bigIntVector == null) {
      int vectorCount = internalMap.size();
      bigIntVector = internalMap.addOrGet("bigInt", BIGINT_TYPE, NullableBigIntVector.class);
      addSubType(MinorType.BIGINT);
      if (internalMap.size() > vectorCount) {
        bigIntVector.allocateNew();
      }
    }
    return bigIntVector;
  }



  private NullableUInt8Vector uInt8Vector;
  private static final MajorType UINT8_TYPE = new MajorType(MinorType.UINT8, DataMode.OPTIONAL);

  public NullableUInt8Vector getUInt8Vector() {
    if (uInt8Vector == null) {
      int vectorCount = internalMap.size();
      uInt8Vector = internalMap.addOrGet("uInt8", UINT8_TYPE, NullableUInt8Vector.class);
      addSubType(MinorType.UINT8);
      if (internalMap.size() > vectorCount) {
        uInt8Vector.allocateNew();
      }
    }
    return uInt8Vector;
  }



  private NullableFloat8Vector float8Vector;
  private static final MajorType FLOAT8_TYPE = new MajorType(MinorType.FLOAT8, DataMode.OPTIONAL);

  public NullableFloat8Vector getFloat8Vector() {
    if (float8Vector == null) {
      int vectorCount = internalMap.size();
      float8Vector = internalMap.addOrGet("float8", FLOAT8_TYPE, NullableFloat8Vector.class);
      addSubType(MinorType.FLOAT8);
      if (internalMap.size() > vectorCount) {
        float8Vector.allocateNew();
      }
    }
    return float8Vector;
  }



  private NullableDateVector dateVector;
  private static final MajorType DATE_TYPE = new MajorType(MinorType.DATE, DataMode.OPTIONAL);

  public NullableDateVector getDateVector() {
    if (dateVector == null) {
      int vectorCount = internalMap.size();
      dateVector = internalMap.addOrGet("date", DATE_TYPE, NullableDateVector.class);
      addSubType(MinorType.DATE);
      if (internalMap.size() > vectorCount) {
        dateVector.allocateNew();
      }
    }
    return dateVector;
  }



  private NullableTimeStampVector timeStampVector;
  private static final MajorType TIMESTAMP_TYPE = new MajorType(MinorType.TIMESTAMP, DataMode.OPTIONAL);

  public NullableTimeStampVector getTimeStampVector() {
    if (timeStampVector == null) {
      int vectorCount = internalMap.size();
      timeStampVector = internalMap.addOrGet("timeStamp", TIMESTAMP_TYPE, NullableTimeStampVector.class);
      addSubType(MinorType.TIMESTAMP);
      if (internalMap.size() > vectorCount) {
        timeStampVector.allocateNew();
      }
    }
    return timeStampVector;
  }




  private NullableIntervalDayVector intervalDayVector;
  private static final MajorType INTERVALDAY_TYPE = new MajorType(MinorType.INTERVALDAY, DataMode.OPTIONAL);

  public NullableIntervalDayVector getIntervalDayVector() {
    if (intervalDayVector == null) {
      int vectorCount = internalMap.size();
      intervalDayVector = internalMap.addOrGet("intervalDay", INTERVALDAY_TYPE, NullableIntervalDayVector.class);
      addSubType(MinorType.INTERVALDAY);
      if (internalMap.size() > vectorCount) {
        intervalDayVector.allocateNew();
      }
    }
    return intervalDayVector;
  }



  private NullableIntervalVector intervalVector;
  private static final MajorType INTERVAL_TYPE = new MajorType(MinorType.INTERVAL, DataMode.OPTIONAL);

  public NullableIntervalVector getIntervalVector() {
    if (intervalVector == null) {
      int vectorCount = internalMap.size();
      intervalVector = internalMap.addOrGet("interval", INTERVAL_TYPE, NullableIntervalVector.class);
      addSubType(MinorType.INTERVAL);
      if (internalMap.size() > vectorCount) {
        intervalVector.allocateNew();
      }
    }
    return intervalVector;
  }







  private NullableVarBinaryVector varBinaryVector;
  private static final MajorType VARBINARY_TYPE = new MajorType(MinorType.VARBINARY, DataMode.OPTIONAL);

  public NullableVarBinaryVector getVarBinaryVector() {
    if (varBinaryVector == null) {
      int vectorCount = internalMap.size();
      varBinaryVector = internalMap.addOrGet("varBinary", VARBINARY_TYPE, NullableVarBinaryVector.class);
      addSubType(MinorType.VARBINARY);
      if (internalMap.size() > vectorCount) {
        varBinaryVector.allocateNew();
      }
    }
    return varBinaryVector;
  }



  private NullableVarCharVector varCharVector;
  private static final MajorType VARCHAR_TYPE = new MajorType(MinorType.VARCHAR, DataMode.OPTIONAL);

  public NullableVarCharVector getVarCharVector() {
    if (varCharVector == null) {
      int vectorCount = internalMap.size();
      varCharVector = internalMap.addOrGet("varChar", VARCHAR_TYPE, NullableVarCharVector.class);
      addSubType(MinorType.VARCHAR);
      if (internalMap.size() > vectorCount) {
        varCharVector.allocateNew();
      }
    }
    return varCharVector;
  }



  private NullableVar16CharVector var16CharVector;
  private static final MajorType VAR16CHAR_TYPE = new MajorType(MinorType.VAR16CHAR, DataMode.OPTIONAL);

  public NullableVar16CharVector getVar16CharVector() {
    if (var16CharVector == null) {
      int vectorCount = internalMap.size();
      var16CharVector = internalMap.addOrGet("var16Char", VAR16CHAR_TYPE, NullableVar16CharVector.class);
      addSubType(MinorType.VAR16CHAR);
      if (internalMap.size() > vectorCount) {
        var16CharVector.allocateNew();
      }
    }
    return var16CharVector;
  }



  private NullableBitVector bitVector;
  private static final MajorType BIT_TYPE = new MajorType(MinorType.BIT, DataMode.OPTIONAL);

  public NullableBitVector getBitVector() {
    if (bitVector == null) {
      int vectorCount = internalMap.size();
      bitVector = internalMap.addOrGet("bit", BIT_TYPE, NullableBitVector.class);
      addSubType(MinorType.BIT);
      if (internalMap.size() > vectorCount) {
        bitVector.allocateNew();
      }
    }
    return bitVector;
  }



  private static final MajorType LIST_TYPE = new MajorType(MinorType.LIST, DataMode.OPTIONAL);

  public ListVector getList() {
    if (listVector == null) {
      int vectorCount = internalMap.size();
      listVector = internalMap.addOrGet("list", LIST_TYPE, ListVector.class);
      addSubType(MinorType.LIST);
      if (internalMap.size() > vectorCount) {
        listVector.allocateNew();
      }
    }
    return listVector;
  }

  public int getTypeValue(int index) {
    return typeVector.getAccessor().get(index);
  }

  public UInt1Vector getTypeVector() {
    return typeVector;
  }

  @Override
  public void allocateNew() throws OutOfMemoryException {
    internalMap.allocateNew();
    if (typeVector != null) {
      typeVector.zeroVector();
    }
  }

  @Override
  public boolean allocateNewSafe() {
    boolean safe = internalMap.allocateNewSafe();
    if (safe) {
      if (typeVector != null) {
        typeVector.zeroVector();
      }
    }
    return safe;
  }

  @Override
  public void setInitialCapacity(int numRecords) {
  }

  @Override
  public int getValueCapacity() {
    return Math.min(typeVector.getValueCapacity(), internalMap.getValueCapacity());
  }

  @Override
  public void close() {
  }

  @Override
  public void clear() {
    internalMap.clear();
  }

  @Override
  public MaterializedField getField() {
    return field;
  }

  @Override
  public TransferPair getTransferPair(BufferAllocator allocator) {
    return new TransferImpl(field, allocator);
  }

  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator) {
    return new TransferImpl(field.withPath(ref), allocator);
  }

  @Override
  public TransferPair makeTransferPair(ValueVector target) {
    return new TransferImpl((UnionVector) target);
  }

  public void transferTo(UnionVector target) {
    internalMap.makeTransferPair(target.internalMap).transfer();
    target.valueCount = valueCount;
    target.majorType = majorType;
  }

  public void copyFrom(int inIndex, int outIndex, UnionVector from) {
    from.getReader().setPosition(inIndex);
    getWriter().setPosition(outIndex);
    ComplexCopier.copy(from.reader, mutator.writer);
  }

  public void copyFromSafe(int inIndex, int outIndex, UnionVector from) {
    copyFrom(inIndex, outIndex, from);
  }

  public ValueVector addVector(ValueVector v) {
    String name = v.getField().getType().getMinorType().name().toLowerCase();
    MajorType type = v.getField().getType();
    Preconditions.checkState(internalMap.getChild(name) == null, String.format("%s vector already exists", name));
    final ValueVector newVector = internalMap.addOrGet(name, type, (Class<ValueVector>) BasicTypeHelper.getValueVectorClass(type.getMinorType(), type.getMode()));
    v.makeTransferPair(newVector).transfer();
    internalMap.putChild(name, newVector);
    addSubType(v.getField().getType().getMinorType());
    return newVector;
  }

  private class TransferImpl implements TransferPair {

    UnionVector to;

    public TransferImpl(MaterializedField field, BufferAllocator allocator) {
      to = new UnionVector(field, allocator, null);
    }

    public TransferImpl(UnionVector to) {
      this.to = to;
    }

    @Override
    public void transfer() {
      transferTo(to);
    }

    @Override
    public void splitAndTransfer(int startIndex, int length) {

    }

    @Override
    public ValueVector getTo() {
      return to;
    }

    @Override
    public void copyValueSafe(int from, int to) {
      this.to.copyFrom(from, to, UnionVector.this);
    }
  }

  @Override
  public Accessor getAccessor() {
    return accessor;
  }

  @Override
  public Mutator getMutator() {
    return mutator;
  }

  @Override
  public FieldReader getReader() {
    if (reader == null) {
      reader = new UnionReader(this);
    }
    return reader;
  }

  public FieldWriter getWriter() {
    if (mutator.writer == null) {
      mutator.writer = new UnionWriter(this);
    }
    return mutator.writer;
  }

//  @Override
//  public UserBitShared.SerializedField getMetadata() {
//    SerializedField.Builder b = getField() //
//            .getAsBuilder() //
//            .setBufferLength(getBufferSize()) //
//            .setValueCount(valueCount);
//
//    b.addChild(internalMap.getMetadata());
//    return b.build();
//  }

  @Override
  public int getBufferSize() {
    return internalMap.getBufferSize();
  }

  @Override
  public int getBufferSizeFor(final int valueCount) {
    if (valueCount == 0) {
      return 0;
    }

    long bufferSize = 0;
    for (final ValueVector v : (Iterable<ValueVector>) this) {
      bufferSize += v.getBufferSizeFor(valueCount);
    }

    return (int) bufferSize;
  }

  @Override
  public ArrowBuf[] getBuffers(boolean clear) {
    return internalMap.getBuffers(clear);
  }

  @Override
  public Iterator<ValueVector> iterator() {
    List<ValueVector> vectors = Lists.newArrayList(internalMap.iterator());
    vectors.add(typeVector);
    return vectors.iterator();
  }

  public class Accessor extends BaseValueVector.BaseAccessor {


    @Override
    public Object getObject(int index) {
      int type = typeVector.getAccessor().get(index);
      switch (MinorType.values()[type]) {
      case LATE:
        return null;
      case TINYINT:
        return getTinyIntVector().getAccessor().getObject(index);

      case UINT1:
        return getUInt1Vector().getAccessor().getObject(index);

      case UINT2:
        return getUInt2Vector().getAccessor().getObject(index);

      case SMALLINT:
        return getSmallIntVector().getAccessor().getObject(index);

      case INT:
        return getIntVector().getAccessor().getObject(index);

      case UINT4:
        return getUInt4Vector().getAccessor().getObject(index);

      case FLOAT4:
        return getFloat4Vector().getAccessor().getObject(index);

      case TIME:
        return getTimeVector().getAccessor().getObject(index);

      case INTERVALYEAR:
        return getIntervalYearVector().getAccessor().getObject(index);


      case BIGINT:
        return getBigIntVector().getAccessor().getObject(index);

      case UINT8:
        return getUInt8Vector().getAccessor().getObject(index);

      case FLOAT8:
        return getFloat8Vector().getAccessor().getObject(index);

      case DATE:
        return getDateVector().getAccessor().getObject(index);

      case TIMESTAMP:
        return getTimeStampVector().getAccessor().getObject(index);


      case INTERVALDAY:
        return getIntervalDayVector().getAccessor().getObject(index);

      case INTERVAL:
        return getIntervalVector().getAccessor().getObject(index);





      case VARBINARY:
        return getVarBinaryVector().getAccessor().getObject(index);

      case VARCHAR:
        return getVarCharVector().getAccessor().getObject(index);

      case VAR16CHAR:
        return getVar16CharVector().getAccessor().getObject(index);

      case BIT:
        return getBitVector().getAccessor().getObject(index);

      case MAP:
        return getMap().getAccessor().getObject(index);
      case LIST:
        return getList().getAccessor().getObject(index);
      default:
        throw new UnsupportedOperationException("Cannot support type: " + MinorType.values()[type]);
      }
    }

    public byte[] get(int index) {
      return null;
    }

    public void get(int index, ComplexHolder holder) {
    }

    public void get(int index, UnionHolder holder) {
      FieldReader reader = new UnionReader(UnionVector.this);
      reader.setPosition(index);
      holder.reader = reader;
    }

    @Override
    public int getValueCount() {
      return valueCount;
    }

    @Override
    public boolean isNull(int index) {
      return typeVector.getAccessor().get(index) == 0;
    }

    public int isSet(int index) {
      return isNull(index) ? 0 : 1;
    }
  }

  public class Mutator extends BaseValueVector.BaseMutator {

    UnionWriter writer;

    @Override
    public void setValueCount(int valueCount) {
      UnionVector.this.valueCount = valueCount;
      internalMap.getMutator().setValueCount(valueCount);
    }

    public void setSafe(int index, UnionHolder holder) {
      FieldReader reader = holder.reader;
      if (writer == null) {
        writer = new UnionWriter(UnionVector.this);
      }
      writer.setPosition(index);
      MinorType type = reader.getType().getMinorType();
      switch (type) {
      case TINYINT:
        NullableTinyIntHolder tinyIntHolder = new NullableTinyIntHolder();
        reader.read(tinyIntHolder);
        setSafe(index, tinyIntHolder);
        break;
      case UINT1:
        NullableUInt1Holder uInt1Holder = new NullableUInt1Holder();
        reader.read(uInt1Holder);
        setSafe(index, uInt1Holder);
        break;
      case UINT2:
        NullableUInt2Holder uInt2Holder = new NullableUInt2Holder();
        reader.read(uInt2Holder);
        setSafe(index, uInt2Holder);
        break;
      case SMALLINT:
        NullableSmallIntHolder smallIntHolder = new NullableSmallIntHolder();
        reader.read(smallIntHolder);
        setSafe(index, smallIntHolder);
        break;
      case INT:
        NullableIntHolder intHolder = new NullableIntHolder();
        reader.read(intHolder);
        setSafe(index, intHolder);
        break;
      case UINT4:
        NullableUInt4Holder uInt4Holder = new NullableUInt4Holder();
        reader.read(uInt4Holder);
        setSafe(index, uInt4Holder);
        break;
      case FLOAT4:
        NullableFloat4Holder float4Holder = new NullableFloat4Holder();
        reader.read(float4Holder);
        setSafe(index, float4Holder);
        break;
      case TIME:
        NullableTimeHolder timeHolder = new NullableTimeHolder();
        reader.read(timeHolder);
        setSafe(index, timeHolder);
        break;
      case INTERVALYEAR:
        NullableIntervalYearHolder intervalYearHolder = new NullableIntervalYearHolder();
        reader.read(intervalYearHolder);
        setSafe(index, intervalYearHolder);
        break;
      case BIGINT:
        NullableBigIntHolder bigIntHolder = new NullableBigIntHolder();
        reader.read(bigIntHolder);
        setSafe(index, bigIntHolder);
        break;
      case UINT8:
        NullableUInt8Holder uInt8Holder = new NullableUInt8Holder();
        reader.read(uInt8Holder);
        setSafe(index, uInt8Holder);
        break;
      case FLOAT8:
        NullableFloat8Holder float8Holder = new NullableFloat8Holder();
        reader.read(float8Holder);
        setSafe(index, float8Holder);
        break;
      case DATE:
        NullableDateHolder dateHolder = new NullableDateHolder();
        reader.read(dateHolder);
        setSafe(index, dateHolder);
        break;
      case TIMESTAMP:
        NullableTimeStampHolder timeStampHolder = new NullableTimeStampHolder();
        reader.read(timeStampHolder);
        setSafe(index, timeStampHolder);
        break;
      case INTERVALDAY:
        NullableIntervalDayHolder intervalDayHolder = new NullableIntervalDayHolder();
        reader.read(intervalDayHolder);
        setSafe(index, intervalDayHolder);
        break;
      case INTERVAL:
        NullableIntervalHolder intervalHolder = new NullableIntervalHolder();
        reader.read(intervalHolder);
        setSafe(index, intervalHolder);
        break;
      case VARBINARY:
        NullableVarBinaryHolder varBinaryHolder = new NullableVarBinaryHolder();
        reader.read(varBinaryHolder);
        setSafe(index, varBinaryHolder);
        break;
      case VARCHAR:
        NullableVarCharHolder varCharHolder = new NullableVarCharHolder();
        reader.read(varCharHolder);
        setSafe(index, varCharHolder);
        break;
      case VAR16CHAR:
        NullableVar16CharHolder var16CharHolder = new NullableVar16CharHolder();
        reader.read(var16CharHolder);
        setSafe(index, var16CharHolder);
        break;
      case BIT:
        NullableBitHolder bitHolder = new NullableBitHolder();
        reader.read(bitHolder);
        setSafe(index, bitHolder);
        break;
      case MAP: {
        ComplexCopier.copy(reader, writer);
        break;
      }
      case LIST: {
        ComplexCopier.copy(reader, writer);
        break;
      }
      default:
        throw new UnsupportedOperationException();
      }
    }

    public void setSafe(int index, NullableTinyIntHolder holder) {
      setType(index, MinorType.TINYINT);
      getTinyIntVector().getMutator().setSafe(index, holder);
    }

    public void setSafe(int index, NullableUInt1Holder holder) {
      setType(index, MinorType.UINT1);
      getUInt1Vector().getMutator().setSafe(index, holder);
    }

    public void setSafe(int index, NullableUInt2Holder holder) {
      setType(index, MinorType.UINT2);
      getUInt2Vector().getMutator().setSafe(index, holder);
    }

    public void setSafe(int index, NullableSmallIntHolder holder) {
      setType(index, MinorType.SMALLINT);
      getSmallIntVector().getMutator().setSafe(index, holder);
    }

    public void setSafe(int index, NullableIntHolder holder) {
      setType(index, MinorType.INT);
      getIntVector().getMutator().setSafe(index, holder);
    }

    public void setSafe(int index, NullableUInt4Holder holder) {
      setType(index, MinorType.UINT4);
      getUInt4Vector().getMutator().setSafe(index, holder);
    }

    public void setSafe(int index, NullableFloat4Holder holder) {
      setType(index, MinorType.FLOAT4);
      getFloat4Vector().getMutator().setSafe(index, holder);
    }

    public void setSafe(int index, NullableTimeHolder holder) {
      setType(index, MinorType.TIME);
      getTimeVector().getMutator().setSafe(index, holder);
    }

    public void setSafe(int index, NullableIntervalYearHolder holder) {
      setType(index, MinorType.INTERVALYEAR);
      getIntervalYearVector().getMutator().setSafe(index, holder);
    }

    public void setSafe(int index, NullableBigIntHolder holder) {
      setType(index, MinorType.BIGINT);
      getBigIntVector().getMutator().setSafe(index, holder);
    }

    public void setSafe(int index, NullableUInt8Holder holder) {
      setType(index, MinorType.UINT8);
      getUInt8Vector().getMutator().setSafe(index, holder);
    }

    public void setSafe(int index, NullableFloat8Holder holder) {
      setType(index, MinorType.FLOAT8);
      getFloat8Vector().getMutator().setSafe(index, holder);
    }

    public void setSafe(int index, NullableDateHolder holder) {
      setType(index, MinorType.DATE);
      getDateVector().getMutator().setSafe(index, holder);
    }

    public void setSafe(int index, NullableTimeStampHolder holder) {
      setType(index, MinorType.TIMESTAMP);
      getTimeStampVector().getMutator().setSafe(index, holder);
    }

    public void setSafe(int index, NullableIntervalDayHolder holder) {
      setType(index, MinorType.INTERVALDAY);
      getIntervalDayVector().getMutator().setSafe(index, holder);
    }

    public void setSafe(int index, NullableIntervalHolder holder) {
      setType(index, MinorType.INTERVAL);
      getIntervalVector().getMutator().setSafe(index, holder);
    }

    public void setSafe(int index, NullableVarBinaryHolder holder) {
      setType(index, MinorType.VARBINARY);
      getVarBinaryVector().getMutator().setSafe(index, holder);
    }

    public void setSafe(int index, NullableVarCharHolder holder) {
      setType(index, MinorType.VARCHAR);
      getVarCharVector().getMutator().setSafe(index, holder);
    }

    public void setSafe(int index, NullableVar16CharHolder holder) {
      setType(index, MinorType.VAR16CHAR);
      getVar16CharVector().getMutator().setSafe(index, holder);
    }

    public void setSafe(int index, NullableBitHolder holder) {
      setType(index, MinorType.BIT);
      getBitVector().getMutator().setSafe(index, holder);
    }


    public void setType(int index, MinorType type) {
      typeVector.getMutator().setSafe(index, type.ordinal());
    }

    @Override
    public void reset() { }

    @Override
    public void generateTestData(int values) { }
  }
}
