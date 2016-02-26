

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
 * This class is generated using FreeMarker and the ListWriters.java template.
 */
@SuppressWarnings("unused")
public class SingleListWriter extends AbstractFieldWriter {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SingleListWriter.class);

  static enum Mode { INIT, IN_MAP, IN_LIST , IN_TINYINT, IN_UINT1, IN_UINT2, IN_SMALLINT, IN_INT, IN_UINT4, IN_FLOAT4, IN_TIME, IN_INTERVALYEAR, IN_DECIMAL9, IN_BIGINT, IN_UINT8, IN_FLOAT8, IN_DATE, IN_TIMESTAMP, IN_DECIMAL18, IN_INTERVALDAY, IN_INTERVAL, IN_DECIMAL28DENSE, IN_DECIMAL38DENSE, IN_DECIMAL38SPARSE, IN_DECIMAL28SPARSE, IN_VARBINARY, IN_VARCHAR, IN_VAR16CHAR, IN_BIT }

  private final String name;
  protected final AbstractContainerVector container;
  private Mode mode = Mode.INIT;
  private FieldWriter writer;
  protected RepeatedValueVector innerVector;

  
  public SingleListWriter(String name, AbstractContainerVector container, FieldWriter parent){
    super(parent);
    this.name = name;
    this.container = container;
  }

  public SingleListWriter(AbstractContainerVector container, FieldWriter parent){
    super(parent);
    this.name = null;
    this.container = container;
  }

  @Override
  public void allocate() {
    if(writer != null) {
      writer.allocate();
    }

  }

  @Override
  public void clear() {
    if (writer != null) {
      writer.clear();
    }
  }

  @Override
  public void close() {
    clear();
    container.close();
    if (innerVector != null) {
      innerVector.close();
    }
  }

  @Override
  public int getValueCapacity() {
    return innerVector == null ? 0 : innerVector.getValueCapacity();
  }

  public void setValueCount(int count){
    if(innerVector != null) innerVector.getMutator().setValueCount(count);
  }

  @Override
  public MapWriter map() {
    switch(mode) {
    case INIT:
      int vectorCount = container.size();
      final RepeatedMapVector vector = container.addOrGet(name, RepeatedMapVector.TYPE, RepeatedMapVector.class);
      innerVector = vector;
      writer = new RepeatedMapWriter(vector, this);
      if(vectorCount != container.size()) {
        writer.allocate();
      }
      writer.setPosition(idx());
      mode = Mode.IN_MAP;
      return writer;
    case IN_MAP:
      return writer;
    }

    throw new RuntimeException(getUnsupportedErrorMsg("MAP", mode.name()));

  }

  @Override
  public ListWriter list() {
    switch(mode) {
    case INIT:
      final int vectorCount = container.size();
      final RepeatedListVector vector = container.addOrGet(name, RepeatedListVector.TYPE, RepeatedListVector.class);
      innerVector = vector;
      writer = new RepeatedListWriter(null, vector, this);
      if(vectorCount != container.size()) {
        writer.allocate();
      }
      writer.setPosition(idx());
      mode = Mode.IN_LIST;
      return writer;
    case IN_LIST:
      return writer;
    }

    throw new RuntimeException(getUnsupportedErrorMsg("LIST", mode.name()));

  }


  private static final MajorType TINYINT_TYPE = Types.repeated(MinorType.TINYINT);

  @Override
  public TinyIntWriter tinyInt() {
    switch(mode) {
    case INIT:
      final int vectorCount = container.size();
      final RepeatedTinyIntVector vector = container.addOrGet(name, TINYINT_TYPE, RepeatedTinyIntVector.class);
      innerVector = vector;
      writer = new RepeatedTinyIntWriterImpl(vector, this);
      if(vectorCount != container.size()) {
        writer.allocate();
      }
      writer.setPosition(idx());
      mode = Mode.IN_TINYINT;
      return writer;
    case IN_TINYINT:
      return writer;
    }

    throw new RuntimeException(getUnsupportedErrorMsg("TINYINT", mode.name()));

  }

  private static final MajorType UINT1_TYPE = Types.repeated(MinorType.UINT1);

  @Override
  public UInt1Writer uInt1() {
    switch(mode) {
    case INIT:
      final int vectorCount = container.size();
      final RepeatedUInt1Vector vector = container.addOrGet(name, UINT1_TYPE, RepeatedUInt1Vector.class);
      innerVector = vector;
      writer = new RepeatedUInt1WriterImpl(vector, this);
      if(vectorCount != container.size()) {
        writer.allocate();
      }
      writer.setPosition(idx());
      mode = Mode.IN_UINT1;
      return writer;
    case IN_UINT1:
      return writer;
    }

    throw new RuntimeException(getUnsupportedErrorMsg("UINT1", mode.name()));

  }

  private static final MajorType UINT2_TYPE = Types.repeated(MinorType.UINT2);

  @Override
  public UInt2Writer uInt2() {
    switch(mode) {
    case INIT:
      final int vectorCount = container.size();
      final RepeatedUInt2Vector vector = container.addOrGet(name, UINT2_TYPE, RepeatedUInt2Vector.class);
      innerVector = vector;
      writer = new RepeatedUInt2WriterImpl(vector, this);
      if(vectorCount != container.size()) {
        writer.allocate();
      }
      writer.setPosition(idx());
      mode = Mode.IN_UINT2;
      return writer;
    case IN_UINT2:
      return writer;
    }

    throw new RuntimeException(getUnsupportedErrorMsg("UINT2", mode.name()));

  }

  private static final MajorType SMALLINT_TYPE = Types.repeated(MinorType.SMALLINT);

  @Override
  public SmallIntWriter smallInt() {
    switch(mode) {
    case INIT:
      final int vectorCount = container.size();
      final RepeatedSmallIntVector vector = container.addOrGet(name, SMALLINT_TYPE, RepeatedSmallIntVector.class);
      innerVector = vector;
      writer = new RepeatedSmallIntWriterImpl(vector, this);
      if(vectorCount != container.size()) {
        writer.allocate();
      }
      writer.setPosition(idx());
      mode = Mode.IN_SMALLINT;
      return writer;
    case IN_SMALLINT:
      return writer;
    }

    throw new RuntimeException(getUnsupportedErrorMsg("SMALLINT", mode.name()));

  }

  private static final MajorType INT_TYPE = Types.repeated(MinorType.INT);

  @Override
  public IntWriter integer() {
    switch(mode) {
    case INIT:
      final int vectorCount = container.size();
      final RepeatedIntVector vector = container.addOrGet(name, INT_TYPE, RepeatedIntVector.class);
      innerVector = vector;
      writer = new RepeatedIntWriterImpl(vector, this);
      if(vectorCount != container.size()) {
        writer.allocate();
      }
      writer.setPosition(idx());
      mode = Mode.IN_INT;
      return writer;
    case IN_INT:
      return writer;
    }

    throw new RuntimeException(getUnsupportedErrorMsg("INT", mode.name()));

  }

  private static final MajorType UINT4_TYPE = Types.repeated(MinorType.UINT4);

  @Override
  public UInt4Writer uInt4() {
    switch(mode) {
    case INIT:
      final int vectorCount = container.size();
      final RepeatedUInt4Vector vector = container.addOrGet(name, UINT4_TYPE, RepeatedUInt4Vector.class);
      innerVector = vector;
      writer = new RepeatedUInt4WriterImpl(vector, this);
      if(vectorCount != container.size()) {
        writer.allocate();
      }
      writer.setPosition(idx());
      mode = Mode.IN_UINT4;
      return writer;
    case IN_UINT4:
      return writer;
    }

    throw new RuntimeException(getUnsupportedErrorMsg("UINT4", mode.name()));

  }

  private static final MajorType FLOAT4_TYPE = Types.repeated(MinorType.FLOAT4);

  @Override
  public Float4Writer float4() {
    switch(mode) {
    case INIT:
      final int vectorCount = container.size();
      final RepeatedFloat4Vector vector = container.addOrGet(name, FLOAT4_TYPE, RepeatedFloat4Vector.class);
      innerVector = vector;
      writer = new RepeatedFloat4WriterImpl(vector, this);
      if(vectorCount != container.size()) {
        writer.allocate();
      }
      writer.setPosition(idx());
      mode = Mode.IN_FLOAT4;
      return writer;
    case IN_FLOAT4:
      return writer;
    }

    throw new RuntimeException(getUnsupportedErrorMsg("FLOAT4", mode.name()));

  }

  private static final MajorType TIME_TYPE = Types.repeated(MinorType.TIME);

  @Override
  public TimeWriter time() {
    switch(mode) {
    case INIT:
      final int vectorCount = container.size();
      final RepeatedTimeVector vector = container.addOrGet(name, TIME_TYPE, RepeatedTimeVector.class);
      innerVector = vector;
      writer = new RepeatedTimeWriterImpl(vector, this);
      if(vectorCount != container.size()) {
        writer.allocate();
      }
      writer.setPosition(idx());
      mode = Mode.IN_TIME;
      return writer;
    case IN_TIME:
      return writer;
    }

    throw new RuntimeException(getUnsupportedErrorMsg("TIME", mode.name()));

  }

  private static final MajorType INTERVALYEAR_TYPE = Types.repeated(MinorType.INTERVALYEAR);

  @Override
  public IntervalYearWriter intervalYear() {
    switch(mode) {
    case INIT:
      final int vectorCount = container.size();
      final RepeatedIntervalYearVector vector = container.addOrGet(name, INTERVALYEAR_TYPE, RepeatedIntervalYearVector.class);
      innerVector = vector;
      writer = new RepeatedIntervalYearWriterImpl(vector, this);
      if(vectorCount != container.size()) {
        writer.allocate();
      }
      writer.setPosition(idx());
      mode = Mode.IN_INTERVALYEAR;
      return writer;
    case IN_INTERVALYEAR:
      return writer;
    }

    throw new RuntimeException(getUnsupportedErrorMsg("INTERVALYEAR", mode.name()));

  }

  private static final MajorType DECIMAL9_TYPE = Types.repeated(MinorType.DECIMAL9);

  @Override
  public Decimal9Writer decimal9() {
    switch(mode) {
    case INIT:
      final int vectorCount = container.size();
      final RepeatedDecimal9Vector vector = container.addOrGet(name, DECIMAL9_TYPE, RepeatedDecimal9Vector.class);
      innerVector = vector;
      writer = new RepeatedDecimal9WriterImpl(vector, this);
      if(vectorCount != container.size()) {
        writer.allocate();
      }
      writer.setPosition(idx());
      mode = Mode.IN_DECIMAL9;
      return writer;
    case IN_DECIMAL9:
      return writer;
    }

    throw new RuntimeException(getUnsupportedErrorMsg("DECIMAL9", mode.name()));

  }

  private static final MajorType BIGINT_TYPE = Types.repeated(MinorType.BIGINT);

  @Override
  public BigIntWriter bigInt() {
    switch(mode) {
    case INIT:
      final int vectorCount = container.size();
      final RepeatedBigIntVector vector = container.addOrGet(name, BIGINT_TYPE, RepeatedBigIntVector.class);
      innerVector = vector;
      writer = new RepeatedBigIntWriterImpl(vector, this);
      if(vectorCount != container.size()) {
        writer.allocate();
      }
      writer.setPosition(idx());
      mode = Mode.IN_BIGINT;
      return writer;
    case IN_BIGINT:
      return writer;
    }

    throw new RuntimeException(getUnsupportedErrorMsg("BIGINT", mode.name()));

  }

  private static final MajorType UINT8_TYPE = Types.repeated(MinorType.UINT8);

  @Override
  public UInt8Writer uInt8() {
    switch(mode) {
    case INIT:
      final int vectorCount = container.size();
      final RepeatedUInt8Vector vector = container.addOrGet(name, UINT8_TYPE, RepeatedUInt8Vector.class);
      innerVector = vector;
      writer = new RepeatedUInt8WriterImpl(vector, this);
      if(vectorCount != container.size()) {
        writer.allocate();
      }
      writer.setPosition(idx());
      mode = Mode.IN_UINT8;
      return writer;
    case IN_UINT8:
      return writer;
    }

    throw new RuntimeException(getUnsupportedErrorMsg("UINT8", mode.name()));

  }

  private static final MajorType FLOAT8_TYPE = Types.repeated(MinorType.FLOAT8);

  @Override
  public Float8Writer float8() {
    switch(mode) {
    case INIT:
      final int vectorCount = container.size();
      final RepeatedFloat8Vector vector = container.addOrGet(name, FLOAT8_TYPE, RepeatedFloat8Vector.class);
      innerVector = vector;
      writer = new RepeatedFloat8WriterImpl(vector, this);
      if(vectorCount != container.size()) {
        writer.allocate();
      }
      writer.setPosition(idx());
      mode = Mode.IN_FLOAT8;
      return writer;
    case IN_FLOAT8:
      return writer;
    }

    throw new RuntimeException(getUnsupportedErrorMsg("FLOAT8", mode.name()));

  }

  private static final MajorType DATE_TYPE = Types.repeated(MinorType.DATE);

  @Override
  public DateWriter date() {
    switch(mode) {
    case INIT:
      final int vectorCount = container.size();
      final RepeatedDateVector vector = container.addOrGet(name, DATE_TYPE, RepeatedDateVector.class);
      innerVector = vector;
      writer = new RepeatedDateWriterImpl(vector, this);
      if(vectorCount != container.size()) {
        writer.allocate();
      }
      writer.setPosition(idx());
      mode = Mode.IN_DATE;
      return writer;
    case IN_DATE:
      return writer;
    }

    throw new RuntimeException(getUnsupportedErrorMsg("DATE", mode.name()));

  }

  private static final MajorType TIMESTAMP_TYPE = Types.repeated(MinorType.TIMESTAMP);

  @Override
  public TimeStampWriter timeStamp() {
    switch(mode) {
    case INIT:
      final int vectorCount = container.size();
      final RepeatedTimeStampVector vector = container.addOrGet(name, TIMESTAMP_TYPE, RepeatedTimeStampVector.class);
      innerVector = vector;
      writer = new RepeatedTimeStampWriterImpl(vector, this);
      if(vectorCount != container.size()) {
        writer.allocate();
      }
      writer.setPosition(idx());
      mode = Mode.IN_TIMESTAMP;
      return writer;
    case IN_TIMESTAMP:
      return writer;
    }

    throw new RuntimeException(getUnsupportedErrorMsg("TIMESTAMP", mode.name()));

  }

  private static final MajorType DECIMAL18_TYPE = Types.repeated(MinorType.DECIMAL18);

  @Override
  public Decimal18Writer decimal18() {
    switch(mode) {
    case INIT:
      final int vectorCount = container.size();
      final RepeatedDecimal18Vector vector = container.addOrGet(name, DECIMAL18_TYPE, RepeatedDecimal18Vector.class);
      innerVector = vector;
      writer = new RepeatedDecimal18WriterImpl(vector, this);
      if(vectorCount != container.size()) {
        writer.allocate();
      }
      writer.setPosition(idx());
      mode = Mode.IN_DECIMAL18;
      return writer;
    case IN_DECIMAL18:
      return writer;
    }

    throw new RuntimeException(getUnsupportedErrorMsg("DECIMAL18", mode.name()));

  }

  private static final MajorType INTERVALDAY_TYPE = Types.repeated(MinorType.INTERVALDAY);

  @Override
  public IntervalDayWriter intervalDay() {
    switch(mode) {
    case INIT:
      final int vectorCount = container.size();
      final RepeatedIntervalDayVector vector = container.addOrGet(name, INTERVALDAY_TYPE, RepeatedIntervalDayVector.class);
      innerVector = vector;
      writer = new RepeatedIntervalDayWriterImpl(vector, this);
      if(vectorCount != container.size()) {
        writer.allocate();
      }
      writer.setPosition(idx());
      mode = Mode.IN_INTERVALDAY;
      return writer;
    case IN_INTERVALDAY:
      return writer;
    }

    throw new RuntimeException(getUnsupportedErrorMsg("INTERVALDAY", mode.name()));

  }

  private static final MajorType INTERVAL_TYPE = Types.repeated(MinorType.INTERVAL);

  @Override
  public IntervalWriter interval() {
    switch(mode) {
    case INIT:
      final int vectorCount = container.size();
      final RepeatedIntervalVector vector = container.addOrGet(name, INTERVAL_TYPE, RepeatedIntervalVector.class);
      innerVector = vector;
      writer = new RepeatedIntervalWriterImpl(vector, this);
      if(vectorCount != container.size()) {
        writer.allocate();
      }
      writer.setPosition(idx());
      mode = Mode.IN_INTERVAL;
      return writer;
    case IN_INTERVAL:
      return writer;
    }

    throw new RuntimeException(getUnsupportedErrorMsg("INTERVAL", mode.name()));

  }

  private static final MajorType DECIMAL28DENSE_TYPE = Types.repeated(MinorType.DECIMAL28DENSE);

  @Override
  public Decimal28DenseWriter decimal28Dense() {
    switch(mode) {
    case INIT:
      final int vectorCount = container.size();
      final RepeatedDecimal28DenseVector vector = container.addOrGet(name, DECIMAL28DENSE_TYPE, RepeatedDecimal28DenseVector.class);
      innerVector = vector;
      writer = new RepeatedDecimal28DenseWriterImpl(vector, this);
      if(vectorCount != container.size()) {
        writer.allocate();
      }
      writer.setPosition(idx());
      mode = Mode.IN_DECIMAL28DENSE;
      return writer;
    case IN_DECIMAL28DENSE:
      return writer;
    }

    throw new RuntimeException(getUnsupportedErrorMsg("DECIMAL28DENSE", mode.name()));

  }

  private static final MajorType DECIMAL38DENSE_TYPE = Types.repeated(MinorType.DECIMAL38DENSE);

  @Override
  public Decimal38DenseWriter decimal38Dense() {
    switch(mode) {
    case INIT:
      final int vectorCount = container.size();
      final RepeatedDecimal38DenseVector vector = container.addOrGet(name, DECIMAL38DENSE_TYPE, RepeatedDecimal38DenseVector.class);
      innerVector = vector;
      writer = new RepeatedDecimal38DenseWriterImpl(vector, this);
      if(vectorCount != container.size()) {
        writer.allocate();
      }
      writer.setPosition(idx());
      mode = Mode.IN_DECIMAL38DENSE;
      return writer;
    case IN_DECIMAL38DENSE:
      return writer;
    }

    throw new RuntimeException(getUnsupportedErrorMsg("DECIMAL38DENSE", mode.name()));

  }

  private static final MajorType DECIMAL38SPARSE_TYPE = Types.repeated(MinorType.DECIMAL38SPARSE);

  @Override
  public Decimal38SparseWriter decimal38Sparse() {
    switch(mode) {
    case INIT:
      final int vectorCount = container.size();
      final RepeatedDecimal38SparseVector vector = container.addOrGet(name, DECIMAL38SPARSE_TYPE, RepeatedDecimal38SparseVector.class);
      innerVector = vector;
      writer = new RepeatedDecimal38SparseWriterImpl(vector, this);
      if(vectorCount != container.size()) {
        writer.allocate();
      }
      writer.setPosition(idx());
      mode = Mode.IN_DECIMAL38SPARSE;
      return writer;
    case IN_DECIMAL38SPARSE:
      return writer;
    }

    throw new RuntimeException(getUnsupportedErrorMsg("DECIMAL38SPARSE", mode.name()));

  }

  private static final MajorType DECIMAL28SPARSE_TYPE = Types.repeated(MinorType.DECIMAL28SPARSE);

  @Override
  public Decimal28SparseWriter decimal28Sparse() {
    switch(mode) {
    case INIT:
      final int vectorCount = container.size();
      final RepeatedDecimal28SparseVector vector = container.addOrGet(name, DECIMAL28SPARSE_TYPE, RepeatedDecimal28SparseVector.class);
      innerVector = vector;
      writer = new RepeatedDecimal28SparseWriterImpl(vector, this);
      if(vectorCount != container.size()) {
        writer.allocate();
      }
      writer.setPosition(idx());
      mode = Mode.IN_DECIMAL28SPARSE;
      return writer;
    case IN_DECIMAL28SPARSE:
      return writer;
    }

    throw new RuntimeException(getUnsupportedErrorMsg("DECIMAL28SPARSE", mode.name()));

  }

  private static final MajorType VARBINARY_TYPE = Types.repeated(MinorType.VARBINARY);

  @Override
  public VarBinaryWriter varBinary() {
    switch(mode) {
    case INIT:
      final int vectorCount = container.size();
      final RepeatedVarBinaryVector vector = container.addOrGet(name, VARBINARY_TYPE, RepeatedVarBinaryVector.class);
      innerVector = vector;
      writer = new RepeatedVarBinaryWriterImpl(vector, this);
      if(vectorCount != container.size()) {
        writer.allocate();
      }
      writer.setPosition(idx());
      mode = Mode.IN_VARBINARY;
      return writer;
    case IN_VARBINARY:
      return writer;
    }

    throw new RuntimeException(getUnsupportedErrorMsg("VARBINARY", mode.name()));

  }

  private static final MajorType VARCHAR_TYPE = Types.repeated(MinorType.VARCHAR);

  @Override
  public VarCharWriter varChar() {
    switch(mode) {
    case INIT:
      final int vectorCount = container.size();
      final RepeatedVarCharVector vector = container.addOrGet(name, VARCHAR_TYPE, RepeatedVarCharVector.class);
      innerVector = vector;
      writer = new RepeatedVarCharWriterImpl(vector, this);
      if(vectorCount != container.size()) {
        writer.allocate();
      }
      writer.setPosition(idx());
      mode = Mode.IN_VARCHAR;
      return writer;
    case IN_VARCHAR:
      return writer;
    }

    throw new RuntimeException(getUnsupportedErrorMsg("VARCHAR", mode.name()));

  }

  private static final MajorType VAR16CHAR_TYPE = Types.repeated(MinorType.VAR16CHAR);

  @Override
  public Var16CharWriter var16Char() {
    switch(mode) {
    case INIT:
      final int vectorCount = container.size();
      final RepeatedVar16CharVector vector = container.addOrGet(name, VAR16CHAR_TYPE, RepeatedVar16CharVector.class);
      innerVector = vector;
      writer = new RepeatedVar16CharWriterImpl(vector, this);
      if(vectorCount != container.size()) {
        writer.allocate();
      }
      writer.setPosition(idx());
      mode = Mode.IN_VAR16CHAR;
      return writer;
    case IN_VAR16CHAR:
      return writer;
    }

    throw new RuntimeException(getUnsupportedErrorMsg("VAR16CHAR", mode.name()));

  }

  private static final MajorType BIT_TYPE = Types.repeated(MinorType.BIT);

  @Override
  public BitWriter bit() {
    switch(mode) {
    case INIT:
      final int vectorCount = container.size();
      final RepeatedBitVector vector = container.addOrGet(name, BIT_TYPE, RepeatedBitVector.class);
      innerVector = vector;
      writer = new RepeatedBitWriterImpl(vector, this);
      if(vectorCount != container.size()) {
        writer.allocate();
      }
      writer.setPosition(idx());
      mode = Mode.IN_BIT;
      return writer;
    case IN_BIT:
      return writer;
    }

    throw new RuntimeException(getUnsupportedErrorMsg("BIT", mode.name()));

  }

  public MaterializedField getField() {
    return container.getField();
  }


  public void setPosition(int index) {
    super.setPosition(index);
    if(writer != null) {
      writer.setPosition(index);
    }
  }

  public void startList() {
    // noop
  }

  public void endList() {
    // noop
  }

  private String getUnsupportedErrorMsg(String expected, String found) {
    final String f = found.substring(3);
    return String.format("In a list of type %s, encountered a value of type %s. "+
      "Arrow does not support lists of different types.",
       f, expected
    );
  }
}
