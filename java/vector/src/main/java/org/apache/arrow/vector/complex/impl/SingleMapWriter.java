
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






import java.util.Map;

import org.apache.arrow.vector.holders.RepeatedMapHolder;
import org.apache.arrow.vector.AllocationHelper;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.complex.writer.FieldWriter;

import com.google.common.collect.Maps;

/*
 * This class is generated using FreeMarker and the MapWriters.java template.
 */
@SuppressWarnings("unused")
public class SingleMapWriter extends AbstractFieldWriter {

  protected final MapVector container;
  private final Map<String, FieldWriter> fields = Maps.newHashMap();
  

  private final boolean unionEnabled;

  public SingleMapWriter(MapVector container, FieldWriter parent, boolean unionEnabled) {
    super(parent);
    this.container = container;
    this.unionEnabled = unionEnabled;
  }

  public SingleMapWriter(MapVector container, FieldWriter parent) {
    this(container, parent, false);
  }

  @Override
  public int getValueCapacity() {
    return container.getValueCapacity();
  }

  @Override
  public boolean isEmptyMap() {
    return 0 == container.size();
  }

  @Override
  public MaterializedField getField() {
      return container.getField();
  }

  @Override
  public MapWriter map(String name) {
      FieldWriter writer = fields.get(name.toLowerCase());
    if(writer == null){
      int vectorCount=container.size();
        MapVector vector = container.addOrGet(name, MapVector.TYPE, MapVector.class);
      if(!unionEnabled){
        writer = new SingleMapWriter(vector, this);
      } else {
        writer = new PromotableWriter(vector, container);
      }
      if(vectorCount != container.size()) {
        writer.allocate();
      }
      writer.setPosition(idx());
      fields.put(name.toLowerCase(), writer);
    }
    return writer;
  }

  @Override
  public void close() throws Exception {
    clear();
    container.close();
  }

  @Override
  public void allocate() {
    container.allocateNew();
    for(final FieldWriter w : fields.values()) {
      w.allocate();
    }
  }

  @Override
  public void clear() {
    container.clear();
    for(final FieldWriter w : fields.values()) {
      w.clear();
    }
  }

  @Override
  public ListWriter list(String name) {
    FieldWriter writer = fields.get(name.toLowerCase());
    int vectorCount = container.size();
    if(writer == null) {
      if (!unionEnabled){
        writer = new SingleListWriter(name,container,this);
      } else{
        writer = new PromotableWriter(container.addOrGet(name, Types.optional(MinorType.LIST), ListVector.class), container);
      }
      if (container.size() > vectorCount) {
        writer.allocate();
      }
      writer.setPosition(idx());
      fields.put(name.toLowerCase(), writer);
    }
    return writer;
  }


  public void setValueCount(int count) {
    container.getMutator().setValueCount(count);
  }

  @Override
  public void setPosition(int index) {
    super.setPosition(index);
    for(final FieldWriter w: fields.values()) {
      w.setPosition(index);
    }
  }

  @Override
  public void start() {
  }

  @Override
  public void end() {
  }



  private static final MajorType TINYINT_TYPE = Types.optional(MinorType.TINYINT);
  @Override
  public TinyIntWriter tinyInt(String name) {
    FieldWriter writer = fields.get(name.toLowerCase());
    if(writer == null) {
      ValueVector vector;
      ValueVector currentVector = container.getChild(name);
      if (unionEnabled){
        NullableTinyIntVector v = container.addOrGet(name, TINYINT_TYPE, NullableTinyIntVector.class);
        writer = new PromotableWriter(v, container);
        vector = v;
      } else {
        NullableTinyIntVector v = container.addOrGet(name, TINYINT_TYPE, NullableTinyIntVector.class);
        writer = new NullableTinyIntWriterImpl(v, this);
        vector = v;
      }
      if (currentVector == null || currentVector != vector) {
        vector.allocateNewSafe();
      } 
      writer.setPosition(idx());
      fields.put(name.toLowerCase(), writer);
    }
    return writer;
  }


  private static final MajorType UINT1_TYPE = Types.optional(MinorType.UINT1);
  @Override
  public UInt1Writer uInt1(String name) {
    FieldWriter writer = fields.get(name.toLowerCase());
    if(writer == null) {
      ValueVector vector;
      ValueVector currentVector = container.getChild(name);
      if (unionEnabled){
        NullableUInt1Vector v = container.addOrGet(name, UINT1_TYPE, NullableUInt1Vector.class);
        writer = new PromotableWriter(v, container);
        vector = v;
      } else {
        NullableUInt1Vector v = container.addOrGet(name, UINT1_TYPE, NullableUInt1Vector.class);
        writer = new NullableUInt1WriterImpl(v, this);
        vector = v;
      }
      if (currentVector == null || currentVector != vector) {
        vector.allocateNewSafe();
      } 
      writer.setPosition(idx());
      fields.put(name.toLowerCase(), writer);
    }
    return writer;
  }


  private static final MajorType UINT2_TYPE = Types.optional(MinorType.UINT2);
  @Override
  public UInt2Writer uInt2(String name) {
    FieldWriter writer = fields.get(name.toLowerCase());
    if(writer == null) {
      ValueVector vector;
      ValueVector currentVector = container.getChild(name);
      if (unionEnabled){
        NullableUInt2Vector v = container.addOrGet(name, UINT2_TYPE, NullableUInt2Vector.class);
        writer = new PromotableWriter(v, container);
        vector = v;
      } else {
        NullableUInt2Vector v = container.addOrGet(name, UINT2_TYPE, NullableUInt2Vector.class);
        writer = new NullableUInt2WriterImpl(v, this);
        vector = v;
      }
      if (currentVector == null || currentVector != vector) {
        vector.allocateNewSafe();
      } 
      writer.setPosition(idx());
      fields.put(name.toLowerCase(), writer);
    }
    return writer;
  }


  private static final MajorType SMALLINT_TYPE = Types.optional(MinorType.SMALLINT);
  @Override
  public SmallIntWriter smallInt(String name) {
    FieldWriter writer = fields.get(name.toLowerCase());
    if(writer == null) {
      ValueVector vector;
      ValueVector currentVector = container.getChild(name);
      if (unionEnabled){
        NullableSmallIntVector v = container.addOrGet(name, SMALLINT_TYPE, NullableSmallIntVector.class);
        writer = new PromotableWriter(v, container);
        vector = v;
      } else {
        NullableSmallIntVector v = container.addOrGet(name, SMALLINT_TYPE, NullableSmallIntVector.class);
        writer = new NullableSmallIntWriterImpl(v, this);
        vector = v;
      }
      if (currentVector == null || currentVector != vector) {
        vector.allocateNewSafe();
      } 
      writer.setPosition(idx());
      fields.put(name.toLowerCase(), writer);
    }
    return writer;
  }


  private static final MajorType INT_TYPE = Types.optional(MinorType.INT);
  @Override
  public IntWriter integer(String name) {
    FieldWriter writer = fields.get(name.toLowerCase());
    if(writer == null) {
      ValueVector vector;
      ValueVector currentVector = container.getChild(name);
      if (unionEnabled){
        NullableIntVector v = container.addOrGet(name, INT_TYPE, NullableIntVector.class);
        writer = new PromotableWriter(v, container);
        vector = v;
      } else {
        NullableIntVector v = container.addOrGet(name, INT_TYPE, NullableIntVector.class);
        writer = new NullableIntWriterImpl(v, this);
        vector = v;
      }
      if (currentVector == null || currentVector != vector) {
        vector.allocateNewSafe();
      } 
      writer.setPosition(idx());
      fields.put(name.toLowerCase(), writer);
    }
    return writer;
  }


  private static final MajorType UINT4_TYPE = Types.optional(MinorType.UINT4);
  @Override
  public UInt4Writer uInt4(String name) {
    FieldWriter writer = fields.get(name.toLowerCase());
    if(writer == null) {
      ValueVector vector;
      ValueVector currentVector = container.getChild(name);
      if (unionEnabled){
        NullableUInt4Vector v = container.addOrGet(name, UINT4_TYPE, NullableUInt4Vector.class);
        writer = new PromotableWriter(v, container);
        vector = v;
      } else {
        NullableUInt4Vector v = container.addOrGet(name, UINT4_TYPE, NullableUInt4Vector.class);
        writer = new NullableUInt4WriterImpl(v, this);
        vector = v;
      }
      if (currentVector == null || currentVector != vector) {
        vector.allocateNewSafe();
      } 
      writer.setPosition(idx());
      fields.put(name.toLowerCase(), writer);
    }
    return writer;
  }


  private static final MajorType FLOAT4_TYPE = Types.optional(MinorType.FLOAT4);
  @Override
  public Float4Writer float4(String name) {
    FieldWriter writer = fields.get(name.toLowerCase());
    if(writer == null) {
      ValueVector vector;
      ValueVector currentVector = container.getChild(name);
      if (unionEnabled){
        NullableFloat4Vector v = container.addOrGet(name, FLOAT4_TYPE, NullableFloat4Vector.class);
        writer = new PromotableWriter(v, container);
        vector = v;
      } else {
        NullableFloat4Vector v = container.addOrGet(name, FLOAT4_TYPE, NullableFloat4Vector.class);
        writer = new NullableFloat4WriterImpl(v, this);
        vector = v;
      }
      if (currentVector == null || currentVector != vector) {
        vector.allocateNewSafe();
      } 
      writer.setPosition(idx());
      fields.put(name.toLowerCase(), writer);
    }
    return writer;
  }


  private static final MajorType TIME_TYPE = Types.optional(MinorType.TIME);
  @Override
  public TimeWriter time(String name) {
    FieldWriter writer = fields.get(name.toLowerCase());
    if(writer == null) {
      ValueVector vector;
      ValueVector currentVector = container.getChild(name);
      if (unionEnabled){
        NullableTimeVector v = container.addOrGet(name, TIME_TYPE, NullableTimeVector.class);
        writer = new PromotableWriter(v, container);
        vector = v;
      } else {
        NullableTimeVector v = container.addOrGet(name, TIME_TYPE, NullableTimeVector.class);
        writer = new NullableTimeWriterImpl(v, this);
        vector = v;
      }
      if (currentVector == null || currentVector != vector) {
        vector.allocateNewSafe();
      } 
      writer.setPosition(idx());
      fields.put(name.toLowerCase(), writer);
    }
    return writer;
  }


  private static final MajorType INTERVALYEAR_TYPE = Types.optional(MinorType.INTERVALYEAR);
  @Override
  public IntervalYearWriter intervalYear(String name) {
    FieldWriter writer = fields.get(name.toLowerCase());
    if(writer == null) {
      ValueVector vector;
      ValueVector currentVector = container.getChild(name);
      if (unionEnabled){
        NullableIntervalYearVector v = container.addOrGet(name, INTERVALYEAR_TYPE, NullableIntervalYearVector.class);
        writer = new PromotableWriter(v, container);
        vector = v;
      } else {
        NullableIntervalYearVector v = container.addOrGet(name, INTERVALYEAR_TYPE, NullableIntervalYearVector.class);
        writer = new NullableIntervalYearWriterImpl(v, this);
        vector = v;
      }
      if (currentVector == null || currentVector != vector) {
        vector.allocateNewSafe();
      } 
      writer.setPosition(idx());
      fields.put(name.toLowerCase(), writer);
    }
    return writer;
  }


  public Decimal9Writer decimal9(String name) {
    // returns existing writer
    final FieldWriter writer = fields.get(name.toLowerCase());
    assert writer != null;
    return writer;
  }

  public Decimal9Writer decimal9(String name, int scale, int precision) {
    final MajorType DECIMAL9_TYPE = new MajorType(MinorType.DECIMAL9, DataMode.OPTIONAL, scale, precision, null, null);
    FieldWriter writer = fields.get(name.toLowerCase());
    if(writer == null) {
      ValueVector vector;
      ValueVector currentVector = container.getChild(name);
      if (unionEnabled){
        NullableDecimal9Vector v = container.addOrGet(name, DECIMAL9_TYPE, NullableDecimal9Vector.class);
        writer = new PromotableWriter(v, container);
        vector = v;
      } else {
        NullableDecimal9Vector v = container.addOrGet(name, DECIMAL9_TYPE, NullableDecimal9Vector.class);
        writer = new NullableDecimal9WriterImpl(v, this);
        vector = v;
      }
      if (currentVector == null || currentVector != vector) {
        vector.allocateNewSafe();
      } 
      writer.setPosition(idx());
      fields.put(name.toLowerCase(), writer);
    }
    return writer;
  }


  private static final MajorType BIGINT_TYPE = Types.optional(MinorType.BIGINT);
  @Override
  public BigIntWriter bigInt(String name) {
    FieldWriter writer = fields.get(name.toLowerCase());
    if(writer == null) {
      ValueVector vector;
      ValueVector currentVector = container.getChild(name);
      if (unionEnabled){
        NullableBigIntVector v = container.addOrGet(name, BIGINT_TYPE, NullableBigIntVector.class);
        writer = new PromotableWriter(v, container);
        vector = v;
      } else {
        NullableBigIntVector v = container.addOrGet(name, BIGINT_TYPE, NullableBigIntVector.class);
        writer = new NullableBigIntWriterImpl(v, this);
        vector = v;
      }
      if (currentVector == null || currentVector != vector) {
        vector.allocateNewSafe();
      } 
      writer.setPosition(idx());
      fields.put(name.toLowerCase(), writer);
    }
    return writer;
  }


  private static final MajorType UINT8_TYPE = Types.optional(MinorType.UINT8);
  @Override
  public UInt8Writer uInt8(String name) {
    FieldWriter writer = fields.get(name.toLowerCase());
    if(writer == null) {
      ValueVector vector;
      ValueVector currentVector = container.getChild(name);
      if (unionEnabled){
        NullableUInt8Vector v = container.addOrGet(name, UINT8_TYPE, NullableUInt8Vector.class);
        writer = new PromotableWriter(v, container);
        vector = v;
      } else {
        NullableUInt8Vector v = container.addOrGet(name, UINT8_TYPE, NullableUInt8Vector.class);
        writer = new NullableUInt8WriterImpl(v, this);
        vector = v;
      }
      if (currentVector == null || currentVector != vector) {
        vector.allocateNewSafe();
      } 
      writer.setPosition(idx());
      fields.put(name.toLowerCase(), writer);
    }
    return writer;
  }


  private static final MajorType FLOAT8_TYPE = Types.optional(MinorType.FLOAT8);
  @Override
  public Float8Writer float8(String name) {
    FieldWriter writer = fields.get(name.toLowerCase());
    if(writer == null) {
      ValueVector vector;
      ValueVector currentVector = container.getChild(name);
      if (unionEnabled){
        NullableFloat8Vector v = container.addOrGet(name, FLOAT8_TYPE, NullableFloat8Vector.class);
        writer = new PromotableWriter(v, container);
        vector = v;
      } else {
        NullableFloat8Vector v = container.addOrGet(name, FLOAT8_TYPE, NullableFloat8Vector.class);
        writer = new NullableFloat8WriterImpl(v, this);
        vector = v;
      }
      if (currentVector == null || currentVector != vector) {
        vector.allocateNewSafe();
      } 
      writer.setPosition(idx());
      fields.put(name.toLowerCase(), writer);
    }
    return writer;
  }


  private static final MajorType DATE_TYPE = Types.optional(MinorType.DATE);
  @Override
  public DateWriter date(String name) {
    FieldWriter writer = fields.get(name.toLowerCase());
    if(writer == null) {
      ValueVector vector;
      ValueVector currentVector = container.getChild(name);
      if (unionEnabled){
        NullableDateVector v = container.addOrGet(name, DATE_TYPE, NullableDateVector.class);
        writer = new PromotableWriter(v, container);
        vector = v;
      } else {
        NullableDateVector v = container.addOrGet(name, DATE_TYPE, NullableDateVector.class);
        writer = new NullableDateWriterImpl(v, this);
        vector = v;
      }
      if (currentVector == null || currentVector != vector) {
        vector.allocateNewSafe();
      } 
      writer.setPosition(idx());
      fields.put(name.toLowerCase(), writer);
    }
    return writer;
  }


  private static final MajorType TIMESTAMP_TYPE = Types.optional(MinorType.TIMESTAMP);
  @Override
  public TimeStampWriter timeStamp(String name) {
    FieldWriter writer = fields.get(name.toLowerCase());
    if(writer == null) {
      ValueVector vector;
      ValueVector currentVector = container.getChild(name);
      if (unionEnabled){
        NullableTimeStampVector v = container.addOrGet(name, TIMESTAMP_TYPE, NullableTimeStampVector.class);
        writer = new PromotableWriter(v, container);
        vector = v;
      } else {
        NullableTimeStampVector v = container.addOrGet(name, TIMESTAMP_TYPE, NullableTimeStampVector.class);
        writer = new NullableTimeStampWriterImpl(v, this);
        vector = v;
      }
      if (currentVector == null || currentVector != vector) {
        vector.allocateNewSafe();
      } 
      writer.setPosition(idx());
      fields.put(name.toLowerCase(), writer);
    }
    return writer;
  }


  public Decimal18Writer decimal18(String name) {
    // returns existing writer
    final FieldWriter writer = fields.get(name.toLowerCase());
    assert writer != null;
    return writer;
  }

  public Decimal18Writer decimal18(String name, int scale, int precision) {
    final MajorType DECIMAL18_TYPE = new MajorType(MinorType.DECIMAL18, DataMode.OPTIONAL, scale, precision, null, null);
    FieldWriter writer = fields.get(name.toLowerCase());
    if(writer == null) {
      ValueVector vector;
      ValueVector currentVector = container.getChild(name);
      if (unionEnabled){
        NullableDecimal18Vector v = container.addOrGet(name, DECIMAL18_TYPE, NullableDecimal18Vector.class);
        writer = new PromotableWriter(v, container);
        vector = v;
      } else {
        NullableDecimal18Vector v = container.addOrGet(name, DECIMAL18_TYPE, NullableDecimal18Vector.class);
        writer = new NullableDecimal18WriterImpl(v, this);
        vector = v;
      }
      if (currentVector == null || currentVector != vector) {
        vector.allocateNewSafe();
      } 
      writer.setPosition(idx());
      fields.put(name.toLowerCase(), writer);
    }
    return writer;
  }


  private static final MajorType INTERVALDAY_TYPE = Types.optional(MinorType.INTERVALDAY);
  @Override
  public IntervalDayWriter intervalDay(String name) {
    FieldWriter writer = fields.get(name.toLowerCase());
    if(writer == null) {
      ValueVector vector;
      ValueVector currentVector = container.getChild(name);
      if (unionEnabled){
        NullableIntervalDayVector v = container.addOrGet(name, INTERVALDAY_TYPE, NullableIntervalDayVector.class);
        writer = new PromotableWriter(v, container);
        vector = v;
      } else {
        NullableIntervalDayVector v = container.addOrGet(name, INTERVALDAY_TYPE, NullableIntervalDayVector.class);
        writer = new NullableIntervalDayWriterImpl(v, this);
        vector = v;
      }
      if (currentVector == null || currentVector != vector) {
        vector.allocateNewSafe();
      } 
      writer.setPosition(idx());
      fields.put(name.toLowerCase(), writer);
    }
    return writer;
  }


  private static final MajorType INTERVAL_TYPE = Types.optional(MinorType.INTERVAL);
  @Override
  public IntervalWriter interval(String name) {
    FieldWriter writer = fields.get(name.toLowerCase());
    if(writer == null) {
      ValueVector vector;
      ValueVector currentVector = container.getChild(name);
      if (unionEnabled){
        NullableIntervalVector v = container.addOrGet(name, INTERVAL_TYPE, NullableIntervalVector.class);
        writer = new PromotableWriter(v, container);
        vector = v;
      } else {
        NullableIntervalVector v = container.addOrGet(name, INTERVAL_TYPE, NullableIntervalVector.class);
        writer = new NullableIntervalWriterImpl(v, this);
        vector = v;
      }
      if (currentVector == null || currentVector != vector) {
        vector.allocateNewSafe();
      } 
      writer.setPosition(idx());
      fields.put(name.toLowerCase(), writer);
    }
    return writer;
  }


  public Decimal28DenseWriter decimal28Dense(String name) {
    // returns existing writer
    final FieldWriter writer = fields.get(name.toLowerCase());
    assert writer != null;
    return writer;
  }

  public Decimal28DenseWriter decimal28Dense(String name, int scale, int precision) {
    final MajorType DECIMAL28DENSE_TYPE = new MajorType(MinorType.DECIMAL28DENSE, DataMode.OPTIONAL, scale, precision, null, null);
    FieldWriter writer = fields.get(name.toLowerCase());
    if(writer == null) {
      ValueVector vector;
      ValueVector currentVector = container.getChild(name);
      if (unionEnabled){
        NullableDecimal28DenseVector v = container.addOrGet(name, DECIMAL28DENSE_TYPE, NullableDecimal28DenseVector.class);
        writer = new PromotableWriter(v, container);
        vector = v;
      } else {
        NullableDecimal28DenseVector v = container.addOrGet(name, DECIMAL28DENSE_TYPE, NullableDecimal28DenseVector.class);
        writer = new NullableDecimal28DenseWriterImpl(v, this);
        vector = v;
      }
      if (currentVector == null || currentVector != vector) {
        vector.allocateNewSafe();
      } 
      writer.setPosition(idx());
      fields.put(name.toLowerCase(), writer);
    }
    return writer;
  }


  public Decimal38DenseWriter decimal38Dense(String name) {
    // returns existing writer
    final FieldWriter writer = fields.get(name.toLowerCase());
    assert writer != null;
    return writer;
  }

  public Decimal38DenseWriter decimal38Dense(String name, int scale, int precision) {
    final MajorType DECIMAL38DENSE_TYPE = new MajorType(MinorType.DECIMAL38DENSE, DataMode.OPTIONAL, scale, precision, null, null);
    FieldWriter writer = fields.get(name.toLowerCase());
    if(writer == null) {
      ValueVector vector;
      ValueVector currentVector = container.getChild(name);
      if (unionEnabled){
        NullableDecimal38DenseVector v = container.addOrGet(name, DECIMAL38DENSE_TYPE, NullableDecimal38DenseVector.class);
        writer = new PromotableWriter(v, container);
        vector = v;
      } else {
        NullableDecimal38DenseVector v = container.addOrGet(name, DECIMAL38DENSE_TYPE, NullableDecimal38DenseVector.class);
        writer = new NullableDecimal38DenseWriterImpl(v, this);
        vector = v;
      }
      if (currentVector == null || currentVector != vector) {
        vector.allocateNewSafe();
      } 
      writer.setPosition(idx());
      fields.put(name.toLowerCase(), writer);
    }
    return writer;
  }


  public Decimal38SparseWriter decimal38Sparse(String name) {
    // returns existing writer
    final FieldWriter writer = fields.get(name.toLowerCase());
    assert writer != null;
    return writer;
  }

  public Decimal38SparseWriter decimal38Sparse(String name, int scale, int precision) {
    final MajorType DECIMAL38SPARSE_TYPE = new MajorType(MinorType.DECIMAL38SPARSE, DataMode.OPTIONAL, scale, precision, null, null);
    FieldWriter writer = fields.get(name.toLowerCase());
    if(writer == null) {
      ValueVector vector;
      ValueVector currentVector = container.getChild(name);
      if (unionEnabled){
        NullableDecimal38SparseVector v = container.addOrGet(name, DECIMAL38SPARSE_TYPE, NullableDecimal38SparseVector.class);
        writer = new PromotableWriter(v, container);
        vector = v;
      } else {
        NullableDecimal38SparseVector v = container.addOrGet(name, DECIMAL38SPARSE_TYPE, NullableDecimal38SparseVector.class);
        writer = new NullableDecimal38SparseWriterImpl(v, this);
        vector = v;
      }
      if (currentVector == null || currentVector != vector) {
        vector.allocateNewSafe();
      } 
      writer.setPosition(idx());
      fields.put(name.toLowerCase(), writer);
    }
    return writer;
  }


  public Decimal28SparseWriter decimal28Sparse(String name) {
    // returns existing writer
    final FieldWriter writer = fields.get(name.toLowerCase());
    assert writer != null;
    return writer;
  }

  public Decimal28SparseWriter decimal28Sparse(String name, int scale, int precision) {
    final MajorType DECIMAL28SPARSE_TYPE = new MajorType(MinorType.DECIMAL28SPARSE, DataMode.OPTIONAL, scale, precision, null, null);
    FieldWriter writer = fields.get(name.toLowerCase());
    if(writer == null) {
      ValueVector vector;
      ValueVector currentVector = container.getChild(name);
      if (unionEnabled){
        NullableDecimal28SparseVector v = container.addOrGet(name, DECIMAL28SPARSE_TYPE, NullableDecimal28SparseVector.class);
        writer = new PromotableWriter(v, container);
        vector = v;
      } else {
        NullableDecimal28SparseVector v = container.addOrGet(name, DECIMAL28SPARSE_TYPE, NullableDecimal28SparseVector.class);
        writer = new NullableDecimal28SparseWriterImpl(v, this);
        vector = v;
      }
      if (currentVector == null || currentVector != vector) {
        vector.allocateNewSafe();
      } 
      writer.setPosition(idx());
      fields.put(name.toLowerCase(), writer);
    }
    return writer;
  }


  private static final MajorType VARBINARY_TYPE = Types.optional(MinorType.VARBINARY);
  @Override
  public VarBinaryWriter varBinary(String name) {
    FieldWriter writer = fields.get(name.toLowerCase());
    if(writer == null) {
      ValueVector vector;
      ValueVector currentVector = container.getChild(name);
      if (unionEnabled){
        NullableVarBinaryVector v = container.addOrGet(name, VARBINARY_TYPE, NullableVarBinaryVector.class);
        writer = new PromotableWriter(v, container);
        vector = v;
      } else {
        NullableVarBinaryVector v = container.addOrGet(name, VARBINARY_TYPE, NullableVarBinaryVector.class);
        writer = new NullableVarBinaryWriterImpl(v, this);
        vector = v;
      }
      if (currentVector == null || currentVector != vector) {
        vector.allocateNewSafe();
      } 
      writer.setPosition(idx());
      fields.put(name.toLowerCase(), writer);
    }
    return writer;
  }


  private static final MajorType VARCHAR_TYPE = Types.optional(MinorType.VARCHAR);
  @Override
  public VarCharWriter varChar(String name) {
    FieldWriter writer = fields.get(name.toLowerCase());
    if(writer == null) {
      ValueVector vector;
      ValueVector currentVector = container.getChild(name);
      if (unionEnabled){
        NullableVarCharVector v = container.addOrGet(name, VARCHAR_TYPE, NullableVarCharVector.class);
        writer = new PromotableWriter(v, container);
        vector = v;
      } else {
        NullableVarCharVector v = container.addOrGet(name, VARCHAR_TYPE, NullableVarCharVector.class);
        writer = new NullableVarCharWriterImpl(v, this);
        vector = v;
      }
      if (currentVector == null || currentVector != vector) {
        vector.allocateNewSafe();
      } 
      writer.setPosition(idx());
      fields.put(name.toLowerCase(), writer);
    }
    return writer;
  }


  private static final MajorType VAR16CHAR_TYPE = Types.optional(MinorType.VAR16CHAR);
  @Override
  public Var16CharWriter var16Char(String name) {
    FieldWriter writer = fields.get(name.toLowerCase());
    if(writer == null) {
      ValueVector vector;
      ValueVector currentVector = container.getChild(name);
      if (unionEnabled){
        NullableVar16CharVector v = container.addOrGet(name, VAR16CHAR_TYPE, NullableVar16CharVector.class);
        writer = new PromotableWriter(v, container);
        vector = v;
      } else {
        NullableVar16CharVector v = container.addOrGet(name, VAR16CHAR_TYPE, NullableVar16CharVector.class);
        writer = new NullableVar16CharWriterImpl(v, this);
        vector = v;
      }
      if (currentVector == null || currentVector != vector) {
        vector.allocateNewSafe();
      } 
      writer.setPosition(idx());
      fields.put(name.toLowerCase(), writer);
    }
    return writer;
  }


  private static final MajorType BIT_TYPE = Types.optional(MinorType.BIT);
  @Override
  public BitWriter bit(String name) {
    FieldWriter writer = fields.get(name.toLowerCase());
    if(writer == null) {
      ValueVector vector;
      ValueVector currentVector = container.getChild(name);
      if (unionEnabled){
        NullableBitVector v = container.addOrGet(name, BIT_TYPE, NullableBitVector.class);
        writer = new PromotableWriter(v, container);
        vector = v;
      } else {
        NullableBitVector v = container.addOrGet(name, BIT_TYPE, NullableBitVector.class);
        writer = new NullableBitWriterImpl(v, this);
        vector = v;
      }
      if (currentVector == null || currentVector != vector) {
        vector.allocateNewSafe();
      } 
      writer.setPosition(idx());
      fields.put(name.toLowerCase(), writer);
    }
    return writer;
  }


}
