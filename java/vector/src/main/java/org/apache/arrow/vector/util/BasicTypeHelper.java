
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
package org.apache.arrow.vector.util;


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






import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.complex.RepeatedMapVector;
import org.apache.arrow.vector.util.CallBack;

public class BasicTypeHelper {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BasicTypeHelper.class);

  private static final int WIDTH_ESTIMATE = 50;

  // Default length when casting to varchar : 65536 = 2^16
  // This only defines an absolute maximum for values, setting
  // a high value like this will not inflate the size for small values
  public static final int VARCHAR_DEFAULT_CAST_LEN = 65536;

  protected static String buildErrorMessage(final String operation, final MinorType type, final DataMode mode) {
    return String.format("Unable to %s for minor type [%s] and mode [%s]", operation, type, mode);
  }

  protected static String buildErrorMessage(final String operation, final MajorType type) {
    return buildErrorMessage(operation, type.getMinorType(), type.getMode());
  }

  public static int getSize(MajorType major) {
    switch (major.getMinorType()) {
    case TINYINT:
      return 1;
    case UINT1:
      return 1;
    case UINT2:
      return 2;
    case SMALLINT:
      return 2;
    case INT:
      return 4;
    case UINT4:
      return 4;
    case FLOAT4:
      return 4;
    case TIME:
      return 4;
    case INTERVALYEAR:
      return 4;
    case DECIMAL9:
      return 4;
    case BIGINT:
      return 8;
    case UINT8:
      return 8;
    case FLOAT8:
      return 8;
    case DATE:
      return 8;
    case TIMESTAMP:
      return 8;
    case DECIMAL18:
      return 8;
    case INTERVALDAY:
      return 12;
    case INTERVAL:
      return 16;
    case DECIMAL28DENSE:
      return 12;
    case DECIMAL38DENSE:
      return 16;
    case DECIMAL38SPARSE:
      return 24;
    case DECIMAL28SPARSE:
      return 20;
    case VARBINARY:
      return 4 + WIDTH_ESTIMATE;
    case VARCHAR:
      return 4 + WIDTH_ESTIMATE;
    case VAR16CHAR:
      return 4 + WIDTH_ESTIMATE;
    case BIT:
      return 1;
//      case FIXEDCHAR: return major.getWidth();
//      case FIXED16CHAR: return major.getWidth();
//      case FIXEDBINARY: return major.getWidth();
    }
    throw new UnsupportedOperationException(buildErrorMessage("get size", major));
  }

  public static ValueVector getNewVector(String name, BufferAllocator allocator, MajorType type, CallBack callback){
    MaterializedField field = MaterializedField.create(name, type);
    return getNewVector(field, allocator, callback);
  }
  
  
  public static Class<?> getValueVectorClass(MinorType type, DataMode mode){
    switch (type) {
    case UNION:
      return UnionVector.class;
    case MAP:
      switch (mode) {
      case OPTIONAL:
      case REQUIRED:
        return MapVector.class;
      case REPEATED:
        return RepeatedMapVector.class;
      }
      
    case LIST:
      switch (mode) {
      case REPEATED:
        return RepeatedListVector.class;
      case REQUIRED:
      case OPTIONAL:
        return ListVector.class;
      }
    
      case TINYINT:
        switch (mode) {
          case REQUIRED:
            return TinyIntVector.class;
          case OPTIONAL:
            return NullableTinyIntVector.class;
          case REPEATED:
            return RepeatedTinyIntVector.class;
        }
      case UINT1:
        switch (mode) {
          case REQUIRED:
            return UInt1Vector.class;
          case OPTIONAL:
            return NullableUInt1Vector.class;
          case REPEATED:
            return RepeatedUInt1Vector.class;
        }
      case UINT2:
        switch (mode) {
          case REQUIRED:
            return UInt2Vector.class;
          case OPTIONAL:
            return NullableUInt2Vector.class;
          case REPEATED:
            return RepeatedUInt2Vector.class;
        }
      case SMALLINT:
        switch (mode) {
          case REQUIRED:
            return SmallIntVector.class;
          case OPTIONAL:
            return NullableSmallIntVector.class;
          case REPEATED:
            return RepeatedSmallIntVector.class;
        }
      case INT:
        switch (mode) {
          case REQUIRED:
            return IntVector.class;
          case OPTIONAL:
            return NullableIntVector.class;
          case REPEATED:
            return RepeatedIntVector.class;
        }
      case UINT4:
        switch (mode) {
          case REQUIRED:
            return UInt4Vector.class;
          case OPTIONAL:
            return NullableUInt4Vector.class;
          case REPEATED:
            return RepeatedUInt4Vector.class;
        }
      case FLOAT4:
        switch (mode) {
          case REQUIRED:
            return Float4Vector.class;
          case OPTIONAL:
            return NullableFloat4Vector.class;
          case REPEATED:
            return RepeatedFloat4Vector.class;
        }
      case TIME:
        switch (mode) {
          case REQUIRED:
            return TimeVector.class;
          case OPTIONAL:
            return NullableTimeVector.class;
          case REPEATED:
            return RepeatedTimeVector.class;
        }
      case INTERVALYEAR:
        switch (mode) {
          case REQUIRED:
            return IntervalYearVector.class;
          case OPTIONAL:
            return NullableIntervalYearVector.class;
          case REPEATED:
            return RepeatedIntervalYearVector.class;
        }
      case DECIMAL9:
        switch (mode) {
          case REQUIRED:
            return Decimal9Vector.class;
          case OPTIONAL:
            return NullableDecimal9Vector.class;
          case REPEATED:
            return RepeatedDecimal9Vector.class;
        }
      case BIGINT:
        switch (mode) {
          case REQUIRED:
            return BigIntVector.class;
          case OPTIONAL:
            return NullableBigIntVector.class;
          case REPEATED:
            return RepeatedBigIntVector.class;
        }
      case UINT8:
        switch (mode) {
          case REQUIRED:
            return UInt8Vector.class;
          case OPTIONAL:
            return NullableUInt8Vector.class;
          case REPEATED:
            return RepeatedUInt8Vector.class;
        }
      case FLOAT8:
        switch (mode) {
          case REQUIRED:
            return Float8Vector.class;
          case OPTIONAL:
            return NullableFloat8Vector.class;
          case REPEATED:
            return RepeatedFloat8Vector.class;
        }
      case DATE:
        switch (mode) {
          case REQUIRED:
            return DateVector.class;
          case OPTIONAL:
            return NullableDateVector.class;
          case REPEATED:
            return RepeatedDateVector.class;
        }
      case TIMESTAMP:
        switch (mode) {
          case REQUIRED:
            return TimeStampVector.class;
          case OPTIONAL:
            return NullableTimeStampVector.class;
          case REPEATED:
            return RepeatedTimeStampVector.class;
        }
      case DECIMAL18:
        switch (mode) {
          case REQUIRED:
            return Decimal18Vector.class;
          case OPTIONAL:
            return NullableDecimal18Vector.class;
          case REPEATED:
            return RepeatedDecimal18Vector.class;
        }
      case INTERVALDAY:
        switch (mode) {
          case REQUIRED:
            return IntervalDayVector.class;
          case OPTIONAL:
            return NullableIntervalDayVector.class;
          case REPEATED:
            return RepeatedIntervalDayVector.class;
        }
      case INTERVAL:
        switch (mode) {
          case REQUIRED:
            return IntervalVector.class;
          case OPTIONAL:
            return NullableIntervalVector.class;
          case REPEATED:
            return RepeatedIntervalVector.class;
        }
      case DECIMAL28DENSE:
        switch (mode) {
          case REQUIRED:
            return Decimal28DenseVector.class;
          case OPTIONAL:
            return NullableDecimal28DenseVector.class;
          case REPEATED:
            return RepeatedDecimal28DenseVector.class;
        }
      case DECIMAL38DENSE:
        switch (mode) {
          case REQUIRED:
            return Decimal38DenseVector.class;
          case OPTIONAL:
            return NullableDecimal38DenseVector.class;
          case REPEATED:
            return RepeatedDecimal38DenseVector.class;
        }
      case DECIMAL38SPARSE:
        switch (mode) {
          case REQUIRED:
            return Decimal38SparseVector.class;
          case OPTIONAL:
            return NullableDecimal38SparseVector.class;
          case REPEATED:
            return RepeatedDecimal38SparseVector.class;
        }
      case DECIMAL28SPARSE:
        switch (mode) {
          case REQUIRED:
            return Decimal28SparseVector.class;
          case OPTIONAL:
            return NullableDecimal28SparseVector.class;
          case REPEATED:
            return RepeatedDecimal28SparseVector.class;
        }
      case VARBINARY:
        switch (mode) {
          case REQUIRED:
            return VarBinaryVector.class;
          case OPTIONAL:
            return NullableVarBinaryVector.class;
          case REPEATED:
            return RepeatedVarBinaryVector.class;
        }
      case VARCHAR:
        switch (mode) {
          case REQUIRED:
            return VarCharVector.class;
          case OPTIONAL:
            return NullableVarCharVector.class;
          case REPEATED:
            return RepeatedVarCharVector.class;
        }
      case VAR16CHAR:
        switch (mode) {
          case REQUIRED:
            return Var16CharVector.class;
          case OPTIONAL:
            return NullableVar16CharVector.class;
          case REPEATED:
            return RepeatedVar16CharVector.class;
        }
      case BIT:
        switch (mode) {
          case REQUIRED:
            return BitVector.class;
          case OPTIONAL:
            return NullableBitVector.class;
          case REPEATED:
            return RepeatedBitVector.class;
        }
    case GENERIC_OBJECT      :
      return ObjectVector.class  ;
    default:
      break;
    }
    throw new UnsupportedOperationException(buildErrorMessage("get value vector class", type, mode));
  }
  public static Class<?> getReaderClassName( MinorType type, DataMode mode, boolean isSingularRepeated){
    switch (type) {
    case MAP:
      switch (mode) {
      case REQUIRED:
        if (!isSingularRepeated)
          return SingleMapReaderImpl.class;
        else
          return SingleLikeRepeatedMapReaderImpl.class;
      case REPEATED: 
          return RepeatedMapReaderImpl.class;
      }
    case LIST:
      switch (mode) {
      case REQUIRED:
        return SingleListReaderImpl.class;
      case REPEATED:
        return RepeatedListReaderImpl.class;
      }
      
      case TINYINT:
        switch (mode) {
          case REQUIRED:
            return TinyIntReaderImpl.class;
          case OPTIONAL:
            return NullableTinyIntReaderImpl.class;
          case REPEATED:
            return RepeatedTinyIntReaderImpl.class;
        }
      case UINT1:
        switch (mode) {
          case REQUIRED:
            return UInt1ReaderImpl.class;
          case OPTIONAL:
            return NullableUInt1ReaderImpl.class;
          case REPEATED:
            return RepeatedUInt1ReaderImpl.class;
        }
      case UINT2:
        switch (mode) {
          case REQUIRED:
            return UInt2ReaderImpl.class;
          case OPTIONAL:
            return NullableUInt2ReaderImpl.class;
          case REPEATED:
            return RepeatedUInt2ReaderImpl.class;
        }
      case SMALLINT:
        switch (mode) {
          case REQUIRED:
            return SmallIntReaderImpl.class;
          case OPTIONAL:
            return NullableSmallIntReaderImpl.class;
          case REPEATED:
            return RepeatedSmallIntReaderImpl.class;
        }
      case INT:
        switch (mode) {
          case REQUIRED:
            return IntReaderImpl.class;
          case OPTIONAL:
            return NullableIntReaderImpl.class;
          case REPEATED:
            return RepeatedIntReaderImpl.class;
        }
      case UINT4:
        switch (mode) {
          case REQUIRED:
            return UInt4ReaderImpl.class;
          case OPTIONAL:
            return NullableUInt4ReaderImpl.class;
          case REPEATED:
            return RepeatedUInt4ReaderImpl.class;
        }
      case FLOAT4:
        switch (mode) {
          case REQUIRED:
            return Float4ReaderImpl.class;
          case OPTIONAL:
            return NullableFloat4ReaderImpl.class;
          case REPEATED:
            return RepeatedFloat4ReaderImpl.class;
        }
      case TIME:
        switch (mode) {
          case REQUIRED:
            return TimeReaderImpl.class;
          case OPTIONAL:
            return NullableTimeReaderImpl.class;
          case REPEATED:
            return RepeatedTimeReaderImpl.class;
        }
      case INTERVALYEAR:
        switch (mode) {
          case REQUIRED:
            return IntervalYearReaderImpl.class;
          case OPTIONAL:
            return NullableIntervalYearReaderImpl.class;
          case REPEATED:
            return RepeatedIntervalYearReaderImpl.class;
        }
      case DECIMAL9:
        switch (mode) {
          case REQUIRED:
            return Decimal9ReaderImpl.class;
          case OPTIONAL:
            return NullableDecimal9ReaderImpl.class;
          case REPEATED:
            return RepeatedDecimal9ReaderImpl.class;
        }
      case BIGINT:
        switch (mode) {
          case REQUIRED:
            return BigIntReaderImpl.class;
          case OPTIONAL:
            return NullableBigIntReaderImpl.class;
          case REPEATED:
            return RepeatedBigIntReaderImpl.class;
        }
      case UINT8:
        switch (mode) {
          case REQUIRED:
            return UInt8ReaderImpl.class;
          case OPTIONAL:
            return NullableUInt8ReaderImpl.class;
          case REPEATED:
            return RepeatedUInt8ReaderImpl.class;
        }
      case FLOAT8:
        switch (mode) {
          case REQUIRED:
            return Float8ReaderImpl.class;
          case OPTIONAL:
            return NullableFloat8ReaderImpl.class;
          case REPEATED:
            return RepeatedFloat8ReaderImpl.class;
        }
      case DATE:
        switch (mode) {
          case REQUIRED:
            return DateReaderImpl.class;
          case OPTIONAL:
            return NullableDateReaderImpl.class;
          case REPEATED:
            return RepeatedDateReaderImpl.class;
        }
      case TIMESTAMP:
        switch (mode) {
          case REQUIRED:
            return TimeStampReaderImpl.class;
          case OPTIONAL:
            return NullableTimeStampReaderImpl.class;
          case REPEATED:
            return RepeatedTimeStampReaderImpl.class;
        }
      case DECIMAL18:
        switch (mode) {
          case REQUIRED:
            return Decimal18ReaderImpl.class;
          case OPTIONAL:
            return NullableDecimal18ReaderImpl.class;
          case REPEATED:
            return RepeatedDecimal18ReaderImpl.class;
        }
      case INTERVALDAY:
        switch (mode) {
          case REQUIRED:
            return IntervalDayReaderImpl.class;
          case OPTIONAL:
            return NullableIntervalDayReaderImpl.class;
          case REPEATED:
            return RepeatedIntervalDayReaderImpl.class;
        }
      case INTERVAL:
        switch (mode) {
          case REQUIRED:
            return IntervalReaderImpl.class;
          case OPTIONAL:
            return NullableIntervalReaderImpl.class;
          case REPEATED:
            return RepeatedIntervalReaderImpl.class;
        }
      case DECIMAL28DENSE:
        switch (mode) {
          case REQUIRED:
            return Decimal28DenseReaderImpl.class;
          case OPTIONAL:
            return NullableDecimal28DenseReaderImpl.class;
          case REPEATED:
            return RepeatedDecimal28DenseReaderImpl.class;
        }
      case DECIMAL38DENSE:
        switch (mode) {
          case REQUIRED:
            return Decimal38DenseReaderImpl.class;
          case OPTIONAL:
            return NullableDecimal38DenseReaderImpl.class;
          case REPEATED:
            return RepeatedDecimal38DenseReaderImpl.class;
        }
      case DECIMAL38SPARSE:
        switch (mode) {
          case REQUIRED:
            return Decimal38SparseReaderImpl.class;
          case OPTIONAL:
            return NullableDecimal38SparseReaderImpl.class;
          case REPEATED:
            return RepeatedDecimal38SparseReaderImpl.class;
        }
      case DECIMAL28SPARSE:
        switch (mode) {
          case REQUIRED:
            return Decimal28SparseReaderImpl.class;
          case OPTIONAL:
            return NullableDecimal28SparseReaderImpl.class;
          case REPEATED:
            return RepeatedDecimal28SparseReaderImpl.class;
        }
      case VARBINARY:
        switch (mode) {
          case REQUIRED:
            return VarBinaryReaderImpl.class;
          case OPTIONAL:
            return NullableVarBinaryReaderImpl.class;
          case REPEATED:
            return RepeatedVarBinaryReaderImpl.class;
        }
      case VARCHAR:
        switch (mode) {
          case REQUIRED:
            return VarCharReaderImpl.class;
          case OPTIONAL:
            return NullableVarCharReaderImpl.class;
          case REPEATED:
            return RepeatedVarCharReaderImpl.class;
        }
      case VAR16CHAR:
        switch (mode) {
          case REQUIRED:
            return Var16CharReaderImpl.class;
          case OPTIONAL:
            return NullableVar16CharReaderImpl.class;
          case REPEATED:
            return RepeatedVar16CharReaderImpl.class;
        }
      case BIT:
        switch (mode) {
          case REQUIRED:
            return BitReaderImpl.class;
          case OPTIONAL:
            return NullableBitReaderImpl.class;
          case REPEATED:
            return RepeatedBitReaderImpl.class;
        }
      default:
        break;
      }
      throw new UnsupportedOperationException(buildErrorMessage("get reader class name", type, mode));
  }
  
  public static Class<?> getWriterInterface( MinorType type, DataMode mode){
    switch (type) {
    case UNION: return UnionWriter.class;
    case MAP: return MapWriter.class;
    case LIST: return ListWriter.class;
      case TINYINT: return TinyIntWriter.class;
      case UINT1: return UInt1Writer.class;
      case UINT2: return UInt2Writer.class;
      case SMALLINT: return SmallIntWriter.class;
      case INT: return IntWriter.class;
      case UINT4: return UInt4Writer.class;
      case FLOAT4: return Float4Writer.class;
      case TIME: return TimeWriter.class;
      case INTERVALYEAR: return IntervalYearWriter.class;
      case DECIMAL9: return Decimal9Writer.class;
      case BIGINT: return BigIntWriter.class;
      case UINT8: return UInt8Writer.class;
      case FLOAT8: return Float8Writer.class;
      case DATE: return DateWriter.class;
      case TIMESTAMP: return TimeStampWriter.class;
      case DECIMAL18: return Decimal18Writer.class;
      case INTERVALDAY: return IntervalDayWriter.class;
      case INTERVAL: return IntervalWriter.class;
      case DECIMAL28DENSE: return Decimal28DenseWriter.class;
      case DECIMAL38DENSE: return Decimal38DenseWriter.class;
      case DECIMAL38SPARSE: return Decimal38SparseWriter.class;
      case DECIMAL28SPARSE: return Decimal28SparseWriter.class;
      case VARBINARY: return VarBinaryWriter.class;
      case VARCHAR: return VarCharWriter.class;
      case VAR16CHAR: return Var16CharWriter.class;
      case BIT: return BitWriter.class;
      default:
        break;
      }
      throw new UnsupportedOperationException(buildErrorMessage("get writer interface", type, mode));
  }
  
  public static Class<?> getWriterImpl( MinorType type, DataMode mode){
    switch (type) {
    case UNION:
      return UnionWriter.class;
    case MAP:
      switch (mode) {
      case REQUIRED:
      case OPTIONAL:
        return SingleMapWriter.class;
      case REPEATED:
        return RepeatedMapWriter.class;
      }
    case LIST:
      switch (mode) {
      case REQUIRED:
      case OPTIONAL:
        return UnionListWriter.class;
      case REPEATED:
        return RepeatedListWriter.class;
      }
      
      case TINYINT:
        switch (mode) {
          case REQUIRED:
            return TinyIntWriterImpl.class;
          case OPTIONAL:
            return NullableTinyIntWriterImpl.class;
          case REPEATED:
            return RepeatedTinyIntWriterImpl.class;
        }
      case UINT1:
        switch (mode) {
          case REQUIRED:
            return UInt1WriterImpl.class;
          case OPTIONAL:
            return NullableUInt1WriterImpl.class;
          case REPEATED:
            return RepeatedUInt1WriterImpl.class;
        }
      case UINT2:
        switch (mode) {
          case REQUIRED:
            return UInt2WriterImpl.class;
          case OPTIONAL:
            return NullableUInt2WriterImpl.class;
          case REPEATED:
            return RepeatedUInt2WriterImpl.class;
        }
      case SMALLINT:
        switch (mode) {
          case REQUIRED:
            return SmallIntWriterImpl.class;
          case OPTIONAL:
            return NullableSmallIntWriterImpl.class;
          case REPEATED:
            return RepeatedSmallIntWriterImpl.class;
        }
      case INT:
        switch (mode) {
          case REQUIRED:
            return IntWriterImpl.class;
          case OPTIONAL:
            return NullableIntWriterImpl.class;
          case REPEATED:
            return RepeatedIntWriterImpl.class;
        }
      case UINT4:
        switch (mode) {
          case REQUIRED:
            return UInt4WriterImpl.class;
          case OPTIONAL:
            return NullableUInt4WriterImpl.class;
          case REPEATED:
            return RepeatedUInt4WriterImpl.class;
        }
      case FLOAT4:
        switch (mode) {
          case REQUIRED:
            return Float4WriterImpl.class;
          case OPTIONAL:
            return NullableFloat4WriterImpl.class;
          case REPEATED:
            return RepeatedFloat4WriterImpl.class;
        }
      case TIME:
        switch (mode) {
          case REQUIRED:
            return TimeWriterImpl.class;
          case OPTIONAL:
            return NullableTimeWriterImpl.class;
          case REPEATED:
            return RepeatedTimeWriterImpl.class;
        }
      case INTERVALYEAR:
        switch (mode) {
          case REQUIRED:
            return IntervalYearWriterImpl.class;
          case OPTIONAL:
            return NullableIntervalYearWriterImpl.class;
          case REPEATED:
            return RepeatedIntervalYearWriterImpl.class;
        }
      case DECIMAL9:
        switch (mode) {
          case REQUIRED:
            return Decimal9WriterImpl.class;
          case OPTIONAL:
            return NullableDecimal9WriterImpl.class;
          case REPEATED:
            return RepeatedDecimal9WriterImpl.class;
        }
      case BIGINT:
        switch (mode) {
          case REQUIRED:
            return BigIntWriterImpl.class;
          case OPTIONAL:
            return NullableBigIntWriterImpl.class;
          case REPEATED:
            return RepeatedBigIntWriterImpl.class;
        }
      case UINT8:
        switch (mode) {
          case REQUIRED:
            return UInt8WriterImpl.class;
          case OPTIONAL:
            return NullableUInt8WriterImpl.class;
          case REPEATED:
            return RepeatedUInt8WriterImpl.class;
        }
      case FLOAT8:
        switch (mode) {
          case REQUIRED:
            return Float8WriterImpl.class;
          case OPTIONAL:
            return NullableFloat8WriterImpl.class;
          case REPEATED:
            return RepeatedFloat8WriterImpl.class;
        }
      case DATE:
        switch (mode) {
          case REQUIRED:
            return DateWriterImpl.class;
          case OPTIONAL:
            return NullableDateWriterImpl.class;
          case REPEATED:
            return RepeatedDateWriterImpl.class;
        }
      case TIMESTAMP:
        switch (mode) {
          case REQUIRED:
            return TimeStampWriterImpl.class;
          case OPTIONAL:
            return NullableTimeStampWriterImpl.class;
          case REPEATED:
            return RepeatedTimeStampWriterImpl.class;
        }
      case DECIMAL18:
        switch (mode) {
          case REQUIRED:
            return Decimal18WriterImpl.class;
          case OPTIONAL:
            return NullableDecimal18WriterImpl.class;
          case REPEATED:
            return RepeatedDecimal18WriterImpl.class;
        }
      case INTERVALDAY:
        switch (mode) {
          case REQUIRED:
            return IntervalDayWriterImpl.class;
          case OPTIONAL:
            return NullableIntervalDayWriterImpl.class;
          case REPEATED:
            return RepeatedIntervalDayWriterImpl.class;
        }
      case INTERVAL:
        switch (mode) {
          case REQUIRED:
            return IntervalWriterImpl.class;
          case OPTIONAL:
            return NullableIntervalWriterImpl.class;
          case REPEATED:
            return RepeatedIntervalWriterImpl.class;
        }
      case DECIMAL28DENSE:
        switch (mode) {
          case REQUIRED:
            return Decimal28DenseWriterImpl.class;
          case OPTIONAL:
            return NullableDecimal28DenseWriterImpl.class;
          case REPEATED:
            return RepeatedDecimal28DenseWriterImpl.class;
        }
      case DECIMAL38DENSE:
        switch (mode) {
          case REQUIRED:
            return Decimal38DenseWriterImpl.class;
          case OPTIONAL:
            return NullableDecimal38DenseWriterImpl.class;
          case REPEATED:
            return RepeatedDecimal38DenseWriterImpl.class;
        }
      case DECIMAL38SPARSE:
        switch (mode) {
          case REQUIRED:
            return Decimal38SparseWriterImpl.class;
          case OPTIONAL:
            return NullableDecimal38SparseWriterImpl.class;
          case REPEATED:
            return RepeatedDecimal38SparseWriterImpl.class;
        }
      case DECIMAL28SPARSE:
        switch (mode) {
          case REQUIRED:
            return Decimal28SparseWriterImpl.class;
          case OPTIONAL:
            return NullableDecimal28SparseWriterImpl.class;
          case REPEATED:
            return RepeatedDecimal28SparseWriterImpl.class;
        }
      case VARBINARY:
        switch (mode) {
          case REQUIRED:
            return VarBinaryWriterImpl.class;
          case OPTIONAL:
            return NullableVarBinaryWriterImpl.class;
          case REPEATED:
            return RepeatedVarBinaryWriterImpl.class;
        }
      case VARCHAR:
        switch (mode) {
          case REQUIRED:
            return VarCharWriterImpl.class;
          case OPTIONAL:
            return NullableVarCharWriterImpl.class;
          case REPEATED:
            return RepeatedVarCharWriterImpl.class;
        }
      case VAR16CHAR:
        switch (mode) {
          case REQUIRED:
            return Var16CharWriterImpl.class;
          case OPTIONAL:
            return NullableVar16CharWriterImpl.class;
          case REPEATED:
            return RepeatedVar16CharWriterImpl.class;
        }
      case BIT:
        switch (mode) {
          case REQUIRED:
            return BitWriterImpl.class;
          case OPTIONAL:
            return NullableBitWriterImpl.class;
          case REPEATED:
            return RepeatedBitWriterImpl.class;
        }
      default:
        break;
      }
      throw new UnsupportedOperationException(buildErrorMessage("get writer implementation", type, mode));
  }

  public static Class<?> getHolderReaderImpl( MinorType type, DataMode mode){
    switch (type) {      
      case TINYINT:
        switch (mode) {
          case REQUIRED:
            return TinyIntHolderReaderImpl.class;
          case OPTIONAL:
            return NullableTinyIntHolderReaderImpl.class;
          case REPEATED:
            return RepeatedTinyIntHolderReaderImpl.class;
        }
      case UINT1:
        switch (mode) {
          case REQUIRED:
            return UInt1HolderReaderImpl.class;
          case OPTIONAL:
            return NullableUInt1HolderReaderImpl.class;
          case REPEATED:
            return RepeatedUInt1HolderReaderImpl.class;
        }
      case UINT2:
        switch (mode) {
          case REQUIRED:
            return UInt2HolderReaderImpl.class;
          case OPTIONAL:
            return NullableUInt2HolderReaderImpl.class;
          case REPEATED:
            return RepeatedUInt2HolderReaderImpl.class;
        }
      case SMALLINT:
        switch (mode) {
          case REQUIRED:
            return SmallIntHolderReaderImpl.class;
          case OPTIONAL:
            return NullableSmallIntHolderReaderImpl.class;
          case REPEATED:
            return RepeatedSmallIntHolderReaderImpl.class;
        }
      case INT:
        switch (mode) {
          case REQUIRED:
            return IntHolderReaderImpl.class;
          case OPTIONAL:
            return NullableIntHolderReaderImpl.class;
          case REPEATED:
            return RepeatedIntHolderReaderImpl.class;
        }
      case UINT4:
        switch (mode) {
          case REQUIRED:
            return UInt4HolderReaderImpl.class;
          case OPTIONAL:
            return NullableUInt4HolderReaderImpl.class;
          case REPEATED:
            return RepeatedUInt4HolderReaderImpl.class;
        }
      case FLOAT4:
        switch (mode) {
          case REQUIRED:
            return Float4HolderReaderImpl.class;
          case OPTIONAL:
            return NullableFloat4HolderReaderImpl.class;
          case REPEATED:
            return RepeatedFloat4HolderReaderImpl.class;
        }
      case TIME:
        switch (mode) {
          case REQUIRED:
            return TimeHolderReaderImpl.class;
          case OPTIONAL:
            return NullableTimeHolderReaderImpl.class;
          case REPEATED:
            return RepeatedTimeHolderReaderImpl.class;
        }
      case INTERVALYEAR:
        switch (mode) {
          case REQUIRED:
            return IntervalYearHolderReaderImpl.class;
          case OPTIONAL:
            return NullableIntervalYearHolderReaderImpl.class;
          case REPEATED:
            return RepeatedIntervalYearHolderReaderImpl.class;
        }
      case DECIMAL9:
        switch (mode) {
          case REQUIRED:
            return Decimal9HolderReaderImpl.class;
          case OPTIONAL:
            return NullableDecimal9HolderReaderImpl.class;
          case REPEATED:
            return RepeatedDecimal9HolderReaderImpl.class;
        }
      case BIGINT:
        switch (mode) {
          case REQUIRED:
            return BigIntHolderReaderImpl.class;
          case OPTIONAL:
            return NullableBigIntHolderReaderImpl.class;
          case REPEATED:
            return RepeatedBigIntHolderReaderImpl.class;
        }
      case UINT8:
        switch (mode) {
          case REQUIRED:
            return UInt8HolderReaderImpl.class;
          case OPTIONAL:
            return NullableUInt8HolderReaderImpl.class;
          case REPEATED:
            return RepeatedUInt8HolderReaderImpl.class;
        }
      case FLOAT8:
        switch (mode) {
          case REQUIRED:
            return Float8HolderReaderImpl.class;
          case OPTIONAL:
            return NullableFloat8HolderReaderImpl.class;
          case REPEATED:
            return RepeatedFloat8HolderReaderImpl.class;
        }
      case DATE:
        switch (mode) {
          case REQUIRED:
            return DateHolderReaderImpl.class;
          case OPTIONAL:
            return NullableDateHolderReaderImpl.class;
          case REPEATED:
            return RepeatedDateHolderReaderImpl.class;
        }
      case TIMESTAMP:
        switch (mode) {
          case REQUIRED:
            return TimeStampHolderReaderImpl.class;
          case OPTIONAL:
            return NullableTimeStampHolderReaderImpl.class;
          case REPEATED:
            return RepeatedTimeStampHolderReaderImpl.class;
        }
      case DECIMAL18:
        switch (mode) {
          case REQUIRED:
            return Decimal18HolderReaderImpl.class;
          case OPTIONAL:
            return NullableDecimal18HolderReaderImpl.class;
          case REPEATED:
            return RepeatedDecimal18HolderReaderImpl.class;
        }
      case INTERVALDAY:
        switch (mode) {
          case REQUIRED:
            return IntervalDayHolderReaderImpl.class;
          case OPTIONAL:
            return NullableIntervalDayHolderReaderImpl.class;
          case REPEATED:
            return RepeatedIntervalDayHolderReaderImpl.class;
        }
      case INTERVAL:
        switch (mode) {
          case REQUIRED:
            return IntervalHolderReaderImpl.class;
          case OPTIONAL:
            return NullableIntervalHolderReaderImpl.class;
          case REPEATED:
            return RepeatedIntervalHolderReaderImpl.class;
        }
      case DECIMAL28DENSE:
        switch (mode) {
          case REQUIRED:
            return Decimal28DenseHolderReaderImpl.class;
          case OPTIONAL:
            return NullableDecimal28DenseHolderReaderImpl.class;
          case REPEATED:
            return RepeatedDecimal28DenseHolderReaderImpl.class;
        }
      case DECIMAL38DENSE:
        switch (mode) {
          case REQUIRED:
            return Decimal38DenseHolderReaderImpl.class;
          case OPTIONAL:
            return NullableDecimal38DenseHolderReaderImpl.class;
          case REPEATED:
            return RepeatedDecimal38DenseHolderReaderImpl.class;
        }
      case DECIMAL38SPARSE:
        switch (mode) {
          case REQUIRED:
            return Decimal38SparseHolderReaderImpl.class;
          case OPTIONAL:
            return NullableDecimal38SparseHolderReaderImpl.class;
          case REPEATED:
            return RepeatedDecimal38SparseHolderReaderImpl.class;
        }
      case DECIMAL28SPARSE:
        switch (mode) {
          case REQUIRED:
            return Decimal28SparseHolderReaderImpl.class;
          case OPTIONAL:
            return NullableDecimal28SparseHolderReaderImpl.class;
          case REPEATED:
            return RepeatedDecimal28SparseHolderReaderImpl.class;
        }
      case VARBINARY:
        switch (mode) {
          case REQUIRED:
            return VarBinaryHolderReaderImpl.class;
          case OPTIONAL:
            return NullableVarBinaryHolderReaderImpl.class;
          case REPEATED:
            return RepeatedVarBinaryHolderReaderImpl.class;
        }
      case VARCHAR:
        switch (mode) {
          case REQUIRED:
            return VarCharHolderReaderImpl.class;
          case OPTIONAL:
            return NullableVarCharHolderReaderImpl.class;
          case REPEATED:
            return RepeatedVarCharHolderReaderImpl.class;
        }
      case VAR16CHAR:
        switch (mode) {
          case REQUIRED:
            return Var16CharHolderReaderImpl.class;
          case OPTIONAL:
            return NullableVar16CharHolderReaderImpl.class;
          case REPEATED:
            return RepeatedVar16CharHolderReaderImpl.class;
        }
      case BIT:
        switch (mode) {
          case REQUIRED:
            return BitHolderReaderImpl.class;
          case OPTIONAL:
            return NullableBitHolderReaderImpl.class;
          case REPEATED:
            return RepeatedBitHolderReaderImpl.class;
        }
      default:
        break;
      }
      throw new UnsupportedOperationException(buildErrorMessage("get holder reader implementation", type, mode));
  }
  
  public static ValueVector getNewVector(MaterializedField field, BufferAllocator allocator){
    return getNewVector(field, allocator, null);
  }
  public static ValueVector getNewVector(MaterializedField field, BufferAllocator allocator, CallBack callBack){
    MajorType type = field.getType();

    switch (type.getMinorType()) {
    
    case UNION:
      return new UnionVector(field, allocator, callBack);

    case MAP:
      switch (type.getMode()) {
      case REQUIRED:
      case OPTIONAL:
        return new MapVector(field, allocator, callBack);
      case REPEATED:
        return new RepeatedMapVector(field, allocator, callBack);
      }
    case LIST:
      switch (type.getMode()) {
      case REPEATED:
        return new RepeatedListVector(field, allocator, callBack);
      case OPTIONAL:
      case REQUIRED:
        return new ListVector(field, allocator, callBack);
      }
    case TINYINT:
      switch (type.getMode()) {
        case REQUIRED:
          return new TinyIntVector(field, allocator);
        case OPTIONAL:
          return new NullableTinyIntVector(field, allocator);
        case REPEATED:
          return new RepeatedTinyIntVector(field, allocator);
      }
    case UINT1:
      switch (type.getMode()) {
        case REQUIRED:
          return new UInt1Vector(field, allocator);
        case OPTIONAL:
          return new NullableUInt1Vector(field, allocator);
        case REPEATED:
          return new RepeatedUInt1Vector(field, allocator);
      }
    case UINT2:
      switch (type.getMode()) {
        case REQUIRED:
          return new UInt2Vector(field, allocator);
        case OPTIONAL:
          return new NullableUInt2Vector(field, allocator);
        case REPEATED:
          return new RepeatedUInt2Vector(field, allocator);
      }
    case SMALLINT:
      switch (type.getMode()) {
        case REQUIRED:
          return new SmallIntVector(field, allocator);
        case OPTIONAL:
          return new NullableSmallIntVector(field, allocator);
        case REPEATED:
          return new RepeatedSmallIntVector(field, allocator);
      }
    case INT:
      switch (type.getMode()) {
        case REQUIRED:
          return new IntVector(field, allocator);
        case OPTIONAL:
          return new NullableIntVector(field, allocator);
        case REPEATED:
          return new RepeatedIntVector(field, allocator);
      }
    case UINT4:
      switch (type.getMode()) {
        case REQUIRED:
          return new UInt4Vector(field, allocator);
        case OPTIONAL:
          return new NullableUInt4Vector(field, allocator);
        case REPEATED:
          return new RepeatedUInt4Vector(field, allocator);
      }
    case FLOAT4:
      switch (type.getMode()) {
        case REQUIRED:
          return new Float4Vector(field, allocator);
        case OPTIONAL:
          return new NullableFloat4Vector(field, allocator);
        case REPEATED:
          return new RepeatedFloat4Vector(field, allocator);
      }
    case TIME:
      switch (type.getMode()) {
        case REQUIRED:
          return new TimeVector(field, allocator);
        case OPTIONAL:
          return new NullableTimeVector(field, allocator);
        case REPEATED:
          return new RepeatedTimeVector(field, allocator);
      }
    case INTERVALYEAR:
      switch (type.getMode()) {
        case REQUIRED:
          return new IntervalYearVector(field, allocator);
        case OPTIONAL:
          return new NullableIntervalYearVector(field, allocator);
        case REPEATED:
          return new RepeatedIntervalYearVector(field, allocator);
      }
    case DECIMAL9:
      switch (type.getMode()) {
        case REQUIRED:
          return new Decimal9Vector(field, allocator);
        case OPTIONAL:
          return new NullableDecimal9Vector(field, allocator);
        case REPEATED:
          return new RepeatedDecimal9Vector(field, allocator);
      }
    case BIGINT:
      switch (type.getMode()) {
        case REQUIRED:
          return new BigIntVector(field, allocator);
        case OPTIONAL:
          return new NullableBigIntVector(field, allocator);
        case REPEATED:
          return new RepeatedBigIntVector(field, allocator);
      }
    case UINT8:
      switch (type.getMode()) {
        case REQUIRED:
          return new UInt8Vector(field, allocator);
        case OPTIONAL:
          return new NullableUInt8Vector(field, allocator);
        case REPEATED:
          return new RepeatedUInt8Vector(field, allocator);
      }
    case FLOAT8:
      switch (type.getMode()) {
        case REQUIRED:
          return new Float8Vector(field, allocator);
        case OPTIONAL:
          return new NullableFloat8Vector(field, allocator);
        case REPEATED:
          return new RepeatedFloat8Vector(field, allocator);
      }
    case DATE:
      switch (type.getMode()) {
        case REQUIRED:
          return new DateVector(field, allocator);
        case OPTIONAL:
          return new NullableDateVector(field, allocator);
        case REPEATED:
          return new RepeatedDateVector(field, allocator);
      }
    case TIMESTAMP:
      switch (type.getMode()) {
        case REQUIRED:
          return new TimeStampVector(field, allocator);
        case OPTIONAL:
          return new NullableTimeStampVector(field, allocator);
        case REPEATED:
          return new RepeatedTimeStampVector(field, allocator);
      }
    case DECIMAL18:
      switch (type.getMode()) {
        case REQUIRED:
          return new Decimal18Vector(field, allocator);
        case OPTIONAL:
          return new NullableDecimal18Vector(field, allocator);
        case REPEATED:
          return new RepeatedDecimal18Vector(field, allocator);
      }
    case INTERVALDAY:
      switch (type.getMode()) {
        case REQUIRED:
          return new IntervalDayVector(field, allocator);
        case OPTIONAL:
          return new NullableIntervalDayVector(field, allocator);
        case REPEATED:
          return new RepeatedIntervalDayVector(field, allocator);
      }
    case INTERVAL:
      switch (type.getMode()) {
        case REQUIRED:
          return new IntervalVector(field, allocator);
        case OPTIONAL:
          return new NullableIntervalVector(field, allocator);
        case REPEATED:
          return new RepeatedIntervalVector(field, allocator);
      }
    case DECIMAL28DENSE:
      switch (type.getMode()) {
        case REQUIRED:
          return new Decimal28DenseVector(field, allocator);
        case OPTIONAL:
          return new NullableDecimal28DenseVector(field, allocator);
        case REPEATED:
          return new RepeatedDecimal28DenseVector(field, allocator);
      }
    case DECIMAL38DENSE:
      switch (type.getMode()) {
        case REQUIRED:
          return new Decimal38DenseVector(field, allocator);
        case OPTIONAL:
          return new NullableDecimal38DenseVector(field, allocator);
        case REPEATED:
          return new RepeatedDecimal38DenseVector(field, allocator);
      }
    case DECIMAL38SPARSE:
      switch (type.getMode()) {
        case REQUIRED:
          return new Decimal38SparseVector(field, allocator);
        case OPTIONAL:
          return new NullableDecimal38SparseVector(field, allocator);
        case REPEATED:
          return new RepeatedDecimal38SparseVector(field, allocator);
      }
    case DECIMAL28SPARSE:
      switch (type.getMode()) {
        case REQUIRED:
          return new Decimal28SparseVector(field, allocator);
        case OPTIONAL:
          return new NullableDecimal28SparseVector(field, allocator);
        case REPEATED:
          return new RepeatedDecimal28SparseVector(field, allocator);
      }
    case VARBINARY:
      switch (type.getMode()) {
        case REQUIRED:
          return new VarBinaryVector(field, allocator);
        case OPTIONAL:
          return new NullableVarBinaryVector(field, allocator);
        case REPEATED:
          return new RepeatedVarBinaryVector(field, allocator);
      }
    case VARCHAR:
      switch (type.getMode()) {
        case REQUIRED:
          return new VarCharVector(field, allocator);
        case OPTIONAL:
          return new NullableVarCharVector(field, allocator);
        case REPEATED:
          return new RepeatedVarCharVector(field, allocator);
      }
    case VAR16CHAR:
      switch (type.getMode()) {
        case REQUIRED:
          return new Var16CharVector(field, allocator);
        case OPTIONAL:
          return new NullableVar16CharVector(field, allocator);
        case REPEATED:
          return new RepeatedVar16CharVector(field, allocator);
      }
    case BIT:
      switch (type.getMode()) {
        case REQUIRED:
          return new BitVector(field, allocator);
        case OPTIONAL:
          return new NullableBitVector(field, allocator);
        case REPEATED:
          return new RepeatedBitVector(field, allocator);
      }
    case GENERIC_OBJECT:
      return new ObjectVector(field, allocator)        ;
    default:
      break;
    }
    // All ValueVector types have been handled.
    throw new UnsupportedOperationException(buildErrorMessage("get new vector", type));
  }

  public static ValueHolder getValue(ValueVector vector, int index) {
    MajorType type = vector.getField().getType();
    ValueHolder holder;
    switch(type.getMinorType()) {
    case TINYINT :
      switch (type.getMode()) {
        case REQUIRED:
          holder = new TinyIntHolder();
          ((TinyIntHolder)holder).value = ((TinyIntVector) vector).getAccessor().get(index);
          return holder;
        case OPTIONAL:
          holder = new NullableTinyIntHolder();
          ((NullableTinyIntHolder)holder).isSet = ((NullableTinyIntVector) vector).getAccessor().isSet(index);
          if (((NullableTinyIntHolder)holder).isSet == 1) {
            ((NullableTinyIntHolder)holder).value = ((NullableTinyIntVector) vector).getAccessor().get(index);
          }
          return holder;
      }
    case UINT1 :
      switch (type.getMode()) {
        case REQUIRED:
          holder = new UInt1Holder();
          ((UInt1Holder)holder).value = ((UInt1Vector) vector).getAccessor().get(index);
          return holder;
        case OPTIONAL:
          holder = new NullableUInt1Holder();
          ((NullableUInt1Holder)holder).isSet = ((NullableUInt1Vector) vector).getAccessor().isSet(index);
          if (((NullableUInt1Holder)holder).isSet == 1) {
            ((NullableUInt1Holder)holder).value = ((NullableUInt1Vector) vector).getAccessor().get(index);
          }
          return holder;
      }
    case UINT2 :
      switch (type.getMode()) {
        case REQUIRED:
          holder = new UInt2Holder();
          ((UInt2Holder)holder).value = ((UInt2Vector) vector).getAccessor().get(index);
          return holder;
        case OPTIONAL:
          holder = new NullableUInt2Holder();
          ((NullableUInt2Holder)holder).isSet = ((NullableUInt2Vector) vector).getAccessor().isSet(index);
          if (((NullableUInt2Holder)holder).isSet == 1) {
            ((NullableUInt2Holder)holder).value = ((NullableUInt2Vector) vector).getAccessor().get(index);
          }
          return holder;
      }
    case SMALLINT :
      switch (type.getMode()) {
        case REQUIRED:
          holder = new SmallIntHolder();
          ((SmallIntHolder)holder).value = ((SmallIntVector) vector).getAccessor().get(index);
          return holder;
        case OPTIONAL:
          holder = new NullableSmallIntHolder();
          ((NullableSmallIntHolder)holder).isSet = ((NullableSmallIntVector) vector).getAccessor().isSet(index);
          if (((NullableSmallIntHolder)holder).isSet == 1) {
            ((NullableSmallIntHolder)holder).value = ((NullableSmallIntVector) vector).getAccessor().get(index);
          }
          return holder;
      }
    case INT :
      switch (type.getMode()) {
        case REQUIRED:
          holder = new IntHolder();
          ((IntHolder)holder).value = ((IntVector) vector).getAccessor().get(index);
          return holder;
        case OPTIONAL:
          holder = new NullableIntHolder();
          ((NullableIntHolder)holder).isSet = ((NullableIntVector) vector).getAccessor().isSet(index);
          if (((NullableIntHolder)holder).isSet == 1) {
            ((NullableIntHolder)holder).value = ((NullableIntVector) vector).getAccessor().get(index);
          }
          return holder;
      }
    case UINT4 :
      switch (type.getMode()) {
        case REQUIRED:
          holder = new UInt4Holder();
          ((UInt4Holder)holder).value = ((UInt4Vector) vector).getAccessor().get(index);
          return holder;
        case OPTIONAL:
          holder = new NullableUInt4Holder();
          ((NullableUInt4Holder)holder).isSet = ((NullableUInt4Vector) vector).getAccessor().isSet(index);
          if (((NullableUInt4Holder)holder).isSet == 1) {
            ((NullableUInt4Holder)holder).value = ((NullableUInt4Vector) vector).getAccessor().get(index);
          }
          return holder;
      }
    case FLOAT4 :
      switch (type.getMode()) {
        case REQUIRED:
          holder = new Float4Holder();
          ((Float4Holder)holder).value = ((Float4Vector) vector).getAccessor().get(index);
          return holder;
        case OPTIONAL:
          holder = new NullableFloat4Holder();
          ((NullableFloat4Holder)holder).isSet = ((NullableFloat4Vector) vector).getAccessor().isSet(index);
          if (((NullableFloat4Holder)holder).isSet == 1) {
            ((NullableFloat4Holder)holder).value = ((NullableFloat4Vector) vector).getAccessor().get(index);
          }
          return holder;
      }
    case TIME :
      switch (type.getMode()) {
        case REQUIRED:
          holder = new TimeHolder();
          ((TimeHolder)holder).value = ((TimeVector) vector).getAccessor().get(index);
          return holder;
        case OPTIONAL:
          holder = new NullableTimeHolder();
          ((NullableTimeHolder)holder).isSet = ((NullableTimeVector) vector).getAccessor().isSet(index);
          if (((NullableTimeHolder)holder).isSet == 1) {
            ((NullableTimeHolder)holder).value = ((NullableTimeVector) vector).getAccessor().get(index);
          }
          return holder;
      }
    case INTERVALYEAR :
      switch (type.getMode()) {
        case REQUIRED:
          holder = new IntervalYearHolder();
          ((IntervalYearHolder)holder).value = ((IntervalYearVector) vector).getAccessor().get(index);
          return holder;
        case OPTIONAL:
          holder = new NullableIntervalYearHolder();
          ((NullableIntervalYearHolder)holder).isSet = ((NullableIntervalYearVector) vector).getAccessor().isSet(index);
          if (((NullableIntervalYearHolder)holder).isSet == 1) {
            ((NullableIntervalYearHolder)holder).value = ((NullableIntervalYearVector) vector).getAccessor().get(index);
          }
          return holder;
      }
    case DECIMAL9 :
      switch (type.getMode()) {
        case REQUIRED:
          holder = new Decimal9Holder();
          ((Decimal9Holder)holder).value = ((Decimal9Vector) vector).getAccessor().get(index);
          return holder;
        case OPTIONAL:
          holder = new NullableDecimal9Holder();
          ((NullableDecimal9Holder)holder).isSet = ((NullableDecimal9Vector) vector).getAccessor().isSet(index);
          if (((NullableDecimal9Holder)holder).isSet == 1) {
            ((NullableDecimal9Holder)holder).value = ((NullableDecimal9Vector) vector).getAccessor().get(index);
          }
          return holder;
      }
    case BIGINT :
      switch (type.getMode()) {
        case REQUIRED:
          holder = new BigIntHolder();
          ((BigIntHolder)holder).value = ((BigIntVector) vector).getAccessor().get(index);
          return holder;
        case OPTIONAL:
          holder = new NullableBigIntHolder();
          ((NullableBigIntHolder)holder).isSet = ((NullableBigIntVector) vector).getAccessor().isSet(index);
          if (((NullableBigIntHolder)holder).isSet == 1) {
            ((NullableBigIntHolder)holder).value = ((NullableBigIntVector) vector).getAccessor().get(index);
          }
          return holder;
      }
    case UINT8 :
      switch (type.getMode()) {
        case REQUIRED:
          holder = new UInt8Holder();
          ((UInt8Holder)holder).value = ((UInt8Vector) vector).getAccessor().get(index);
          return holder;
        case OPTIONAL:
          holder = new NullableUInt8Holder();
          ((NullableUInt8Holder)holder).isSet = ((NullableUInt8Vector) vector).getAccessor().isSet(index);
          if (((NullableUInt8Holder)holder).isSet == 1) {
            ((NullableUInt8Holder)holder).value = ((NullableUInt8Vector) vector).getAccessor().get(index);
          }
          return holder;
      }
    case FLOAT8 :
      switch (type.getMode()) {
        case REQUIRED:
          holder = new Float8Holder();
          ((Float8Holder)holder).value = ((Float8Vector) vector).getAccessor().get(index);
          return holder;
        case OPTIONAL:
          holder = new NullableFloat8Holder();
          ((NullableFloat8Holder)holder).isSet = ((NullableFloat8Vector) vector).getAccessor().isSet(index);
          if (((NullableFloat8Holder)holder).isSet == 1) {
            ((NullableFloat8Holder)holder).value = ((NullableFloat8Vector) vector).getAccessor().get(index);
          }
          return holder;
      }
    case DATE :
      switch (type.getMode()) {
        case REQUIRED:
          holder = new DateHolder();
          ((DateHolder)holder).value = ((DateVector) vector).getAccessor().get(index);
          return holder;
        case OPTIONAL:
          holder = new NullableDateHolder();
          ((NullableDateHolder)holder).isSet = ((NullableDateVector) vector).getAccessor().isSet(index);
          if (((NullableDateHolder)holder).isSet == 1) {
            ((NullableDateHolder)holder).value = ((NullableDateVector) vector).getAccessor().get(index);
          }
          return holder;
      }
    case TIMESTAMP :
      switch (type.getMode()) {
        case REQUIRED:
          holder = new TimeStampHolder();
          ((TimeStampHolder)holder).value = ((TimeStampVector) vector).getAccessor().get(index);
          return holder;
        case OPTIONAL:
          holder = new NullableTimeStampHolder();
          ((NullableTimeStampHolder)holder).isSet = ((NullableTimeStampVector) vector).getAccessor().isSet(index);
          if (((NullableTimeStampHolder)holder).isSet == 1) {
            ((NullableTimeStampHolder)holder).value = ((NullableTimeStampVector) vector).getAccessor().get(index);
          }
          return holder;
      }
    case DECIMAL18 :
      switch (type.getMode()) {
        case REQUIRED:
          holder = new Decimal18Holder();
          ((Decimal18Holder)holder).value = ((Decimal18Vector) vector).getAccessor().get(index);
          return holder;
        case OPTIONAL:
          holder = new NullableDecimal18Holder();
          ((NullableDecimal18Holder)holder).isSet = ((NullableDecimal18Vector) vector).getAccessor().isSet(index);
          if (((NullableDecimal18Holder)holder).isSet == 1) {
            ((NullableDecimal18Holder)holder).value = ((NullableDecimal18Vector) vector).getAccessor().get(index);
          }
          return holder;
      }
    case INTERVALDAY :
        switch (type.getMode()) {
          case REQUIRED:
            holder = new IntervalDayHolder();
            ((IntervalDayVector) vector).getAccessor().get(index, (IntervalDayHolder)holder);
            return holder;
          case OPTIONAL:
            holder = new NullableIntervalDayHolder();
            ((NullableIntervalDayHolder)holder).isSet = ((NullableIntervalDayVector) vector).getAccessor().isSet(index);
            if (((NullableIntervalDayHolder)holder).isSet == 1) {
              ((NullableIntervalDayVector) vector).getAccessor().get(index, (NullableIntervalDayHolder)holder);
            }
            return holder;
        }
    case INTERVAL :
        switch (type.getMode()) {
          case REQUIRED:
            holder = new IntervalHolder();
            ((IntervalVector) vector).getAccessor().get(index, (IntervalHolder)holder);
            return holder;
          case OPTIONAL:
            holder = new NullableIntervalHolder();
            ((NullableIntervalHolder)holder).isSet = ((NullableIntervalVector) vector).getAccessor().isSet(index);
            if (((NullableIntervalHolder)holder).isSet == 1) {
              ((NullableIntervalVector) vector).getAccessor().get(index, (NullableIntervalHolder)holder);
            }
            return holder;
        }
    case DECIMAL28DENSE :
        switch (type.getMode()) {
          case REQUIRED:
            holder = new Decimal28DenseHolder();
            ((Decimal28DenseVector) vector).getAccessor().get(index, (Decimal28DenseHolder)holder);
            return holder;
          case OPTIONAL:
            holder = new NullableDecimal28DenseHolder();
            ((NullableDecimal28DenseHolder)holder).isSet = ((NullableDecimal28DenseVector) vector).getAccessor().isSet(index);
            if (((NullableDecimal28DenseHolder)holder).isSet == 1) {
              ((NullableDecimal28DenseVector) vector).getAccessor().get(index, (NullableDecimal28DenseHolder)holder);
            }
            return holder;
        }
    case DECIMAL38DENSE :
        switch (type.getMode()) {
          case REQUIRED:
            holder = new Decimal38DenseHolder();
            ((Decimal38DenseVector) vector).getAccessor().get(index, (Decimal38DenseHolder)holder);
            return holder;
          case OPTIONAL:
            holder = new NullableDecimal38DenseHolder();
            ((NullableDecimal38DenseHolder)holder).isSet = ((NullableDecimal38DenseVector) vector).getAccessor().isSet(index);
            if (((NullableDecimal38DenseHolder)holder).isSet == 1) {
              ((NullableDecimal38DenseVector) vector).getAccessor().get(index, (NullableDecimal38DenseHolder)holder);
            }
            return holder;
        }
    case DECIMAL38SPARSE :
        switch (type.getMode()) {
          case REQUIRED:
            holder = new Decimal38SparseHolder();
            ((Decimal38SparseVector) vector).getAccessor().get(index, (Decimal38SparseHolder)holder);
            return holder;
          case OPTIONAL:
            holder = new NullableDecimal38SparseHolder();
            ((NullableDecimal38SparseHolder)holder).isSet = ((NullableDecimal38SparseVector) vector).getAccessor().isSet(index);
            if (((NullableDecimal38SparseHolder)holder).isSet == 1) {
              ((NullableDecimal38SparseVector) vector).getAccessor().get(index, (NullableDecimal38SparseHolder)holder);
            }
            return holder;
        }
    case DECIMAL28SPARSE :
        switch (type.getMode()) {
          case REQUIRED:
            holder = new Decimal28SparseHolder();
            ((Decimal28SparseVector) vector).getAccessor().get(index, (Decimal28SparseHolder)holder);
            return holder;
          case OPTIONAL:
            holder = new NullableDecimal28SparseHolder();
            ((NullableDecimal28SparseHolder)holder).isSet = ((NullableDecimal28SparseVector) vector).getAccessor().isSet(index);
            if (((NullableDecimal28SparseHolder)holder).isSet == 1) {
              ((NullableDecimal28SparseVector) vector).getAccessor().get(index, (NullableDecimal28SparseHolder)holder);
            }
            return holder;
        }
    case VARBINARY :
        switch (type.getMode()) {
          case REQUIRED:
            holder = new VarBinaryHolder();
            ((VarBinaryVector) vector).getAccessor().get(index, (VarBinaryHolder)holder);
            return holder;
          case OPTIONAL:
            holder = new NullableVarBinaryHolder();
            ((NullableVarBinaryHolder)holder).isSet = ((NullableVarBinaryVector) vector).getAccessor().isSet(index);
            if (((NullableVarBinaryHolder)holder).isSet == 1) {
              ((NullableVarBinaryVector) vector).getAccessor().get(index, (NullableVarBinaryHolder)holder);
            }
            return holder;
        }
    case VARCHAR :
        switch (type.getMode()) {
          case REQUIRED:
            holder = new VarCharHolder();
            ((VarCharVector) vector).getAccessor().get(index, (VarCharHolder)holder);
            return holder;
          case OPTIONAL:
            holder = new NullableVarCharHolder();
            ((NullableVarCharHolder)holder).isSet = ((NullableVarCharVector) vector).getAccessor().isSet(index);
            if (((NullableVarCharHolder)holder).isSet == 1) {
              ((NullableVarCharVector) vector).getAccessor().get(index, (NullableVarCharHolder)holder);
            }
            return holder;
        }
    case VAR16CHAR :
        switch (type.getMode()) {
          case REQUIRED:
            holder = new Var16CharHolder();
            ((Var16CharVector) vector).getAccessor().get(index, (Var16CharHolder)holder);
            return holder;
          case OPTIONAL:
            holder = new NullableVar16CharHolder();
            ((NullableVar16CharHolder)holder).isSet = ((NullableVar16CharVector) vector).getAccessor().isSet(index);
            if (((NullableVar16CharHolder)holder).isSet == 1) {
              ((NullableVar16CharVector) vector).getAccessor().get(index, (NullableVar16CharHolder)holder);
            }
            return holder;
        }
    case BIT :
      switch (type.getMode()) {
        case REQUIRED:
          holder = new BitHolder();
          ((BitHolder)holder).value = ((BitVector) vector).getAccessor().get(index);
          return holder;
        case OPTIONAL:
          holder = new NullableBitHolder();
          ((NullableBitHolder)holder).isSet = ((NullableBitVector) vector).getAccessor().isSet(index);
          if (((NullableBitHolder)holder).isSet == 1) {
            ((NullableBitHolder)holder).value = ((NullableBitVector) vector).getAccessor().get(index);
          }
          return holder;
      }
    case GENERIC_OBJECT:
      holder = new ObjectHolder();
      ((ObjectHolder)holder).obj = ((ObjectVector) vector).getAccessor().getObject(index)         ;
      break;
    }

    throw new UnsupportedOperationException(buildErrorMessage("get value", type));
  }

  public static void setValue(ValueVector vector, int index, ValueHolder holder) {
    MajorType type = vector.getField().getType();

    switch(type.getMinorType()) {
    case TINYINT :
      switch (type.getMode()) {
        case REQUIRED:
          ((TinyIntVector) vector).getMutator().setSafe(index, (TinyIntHolder) holder);
          return;
        case OPTIONAL:
          if (((NullableTinyIntHolder) holder).isSet == 1) {
            ((NullableTinyIntVector) vector).getMutator().setSafe(index, (NullableTinyIntHolder) holder);
          }
          return;
      }
    case UINT1 :
      switch (type.getMode()) {
        case REQUIRED:
          ((UInt1Vector) vector).getMutator().setSafe(index, (UInt1Holder) holder);
          return;
        case OPTIONAL:
          if (((NullableUInt1Holder) holder).isSet == 1) {
            ((NullableUInt1Vector) vector).getMutator().setSafe(index, (NullableUInt1Holder) holder);
          }
          return;
      }
    case UINT2 :
      switch (type.getMode()) {
        case REQUIRED:
          ((UInt2Vector) vector).getMutator().setSafe(index, (UInt2Holder) holder);
          return;
        case OPTIONAL:
          if (((NullableUInt2Holder) holder).isSet == 1) {
            ((NullableUInt2Vector) vector).getMutator().setSafe(index, (NullableUInt2Holder) holder);
          }
          return;
      }
    case SMALLINT :
      switch (type.getMode()) {
        case REQUIRED:
          ((SmallIntVector) vector).getMutator().setSafe(index, (SmallIntHolder) holder);
          return;
        case OPTIONAL:
          if (((NullableSmallIntHolder) holder).isSet == 1) {
            ((NullableSmallIntVector) vector).getMutator().setSafe(index, (NullableSmallIntHolder) holder);
          }
          return;
      }
    case INT :
      switch (type.getMode()) {
        case REQUIRED:
          ((IntVector) vector).getMutator().setSafe(index, (IntHolder) holder);
          return;
        case OPTIONAL:
          if (((NullableIntHolder) holder).isSet == 1) {
            ((NullableIntVector) vector).getMutator().setSafe(index, (NullableIntHolder) holder);
          }
          return;
      }
    case UINT4 :
      switch (type.getMode()) {
        case REQUIRED:
          ((UInt4Vector) vector).getMutator().setSafe(index, (UInt4Holder) holder);
          return;
        case OPTIONAL:
          if (((NullableUInt4Holder) holder).isSet == 1) {
            ((NullableUInt4Vector) vector).getMutator().setSafe(index, (NullableUInt4Holder) holder);
          }
          return;
      }
    case FLOAT4 :
      switch (type.getMode()) {
        case REQUIRED:
          ((Float4Vector) vector).getMutator().setSafe(index, (Float4Holder) holder);
          return;
        case OPTIONAL:
          if (((NullableFloat4Holder) holder).isSet == 1) {
            ((NullableFloat4Vector) vector).getMutator().setSafe(index, (NullableFloat4Holder) holder);
          }
          return;
      }
    case TIME :
      switch (type.getMode()) {
        case REQUIRED:
          ((TimeVector) vector).getMutator().setSafe(index, (TimeHolder) holder);
          return;
        case OPTIONAL:
          if (((NullableTimeHolder) holder).isSet == 1) {
            ((NullableTimeVector) vector).getMutator().setSafe(index, (NullableTimeHolder) holder);
          }
          return;
      }
    case INTERVALYEAR :
      switch (type.getMode()) {
        case REQUIRED:
          ((IntervalYearVector) vector).getMutator().setSafe(index, (IntervalYearHolder) holder);
          return;
        case OPTIONAL:
          if (((NullableIntervalYearHolder) holder).isSet == 1) {
            ((NullableIntervalYearVector) vector).getMutator().setSafe(index, (NullableIntervalYearHolder) holder);
          }
          return;
      }
    case DECIMAL9 :
      switch (type.getMode()) {
        case REQUIRED:
          ((Decimal9Vector) vector).getMutator().setSafe(index, (Decimal9Holder) holder);
          return;
        case OPTIONAL:
          if (((NullableDecimal9Holder) holder).isSet == 1) {
            ((NullableDecimal9Vector) vector).getMutator().setSafe(index, (NullableDecimal9Holder) holder);
          }
          return;
      }
    case BIGINT :
      switch (type.getMode()) {
        case REQUIRED:
          ((BigIntVector) vector).getMutator().setSafe(index, (BigIntHolder) holder);
          return;
        case OPTIONAL:
          if (((NullableBigIntHolder) holder).isSet == 1) {
            ((NullableBigIntVector) vector).getMutator().setSafe(index, (NullableBigIntHolder) holder);
          }
          return;
      }
    case UINT8 :
      switch (type.getMode()) {
        case REQUIRED:
          ((UInt8Vector) vector).getMutator().setSafe(index, (UInt8Holder) holder);
          return;
        case OPTIONAL:
          if (((NullableUInt8Holder) holder).isSet == 1) {
            ((NullableUInt8Vector) vector).getMutator().setSafe(index, (NullableUInt8Holder) holder);
          }
          return;
      }
    case FLOAT8 :
      switch (type.getMode()) {
        case REQUIRED:
          ((Float8Vector) vector).getMutator().setSafe(index, (Float8Holder) holder);
          return;
        case OPTIONAL:
          if (((NullableFloat8Holder) holder).isSet == 1) {
            ((NullableFloat8Vector) vector).getMutator().setSafe(index, (NullableFloat8Holder) holder);
          }
          return;
      }
    case DATE :
      switch (type.getMode()) {
        case REQUIRED:
          ((DateVector) vector).getMutator().setSafe(index, (DateHolder) holder);
          return;
        case OPTIONAL:
          if (((NullableDateHolder) holder).isSet == 1) {
            ((NullableDateVector) vector).getMutator().setSafe(index, (NullableDateHolder) holder);
          }
          return;
      }
    case TIMESTAMP :
      switch (type.getMode()) {
        case REQUIRED:
          ((TimeStampVector) vector).getMutator().setSafe(index, (TimeStampHolder) holder);
          return;
        case OPTIONAL:
          if (((NullableTimeStampHolder) holder).isSet == 1) {
            ((NullableTimeStampVector) vector).getMutator().setSafe(index, (NullableTimeStampHolder) holder);
          }
          return;
      }
    case DECIMAL18 :
      switch (type.getMode()) {
        case REQUIRED:
          ((Decimal18Vector) vector).getMutator().setSafe(index, (Decimal18Holder) holder);
          return;
        case OPTIONAL:
          if (((NullableDecimal18Holder) holder).isSet == 1) {
            ((NullableDecimal18Vector) vector).getMutator().setSafe(index, (NullableDecimal18Holder) holder);
          }
          return;
      }
    case INTERVALDAY :
      switch (type.getMode()) {
        case REQUIRED:
          ((IntervalDayVector) vector).getMutator().setSafe(index, (IntervalDayHolder) holder);
          return;
        case OPTIONAL:
          if (((NullableIntervalDayHolder) holder).isSet == 1) {
            ((NullableIntervalDayVector) vector).getMutator().setSafe(index, (NullableIntervalDayHolder) holder);
          }
          return;
      }
    case INTERVAL :
      switch (type.getMode()) {
        case REQUIRED:
          ((IntervalVector) vector).getMutator().setSafe(index, (IntervalHolder) holder);
          return;
        case OPTIONAL:
          if (((NullableIntervalHolder) holder).isSet == 1) {
            ((NullableIntervalVector) vector).getMutator().setSafe(index, (NullableIntervalHolder) holder);
          }
          return;
      }
    case DECIMAL28DENSE :
      switch (type.getMode()) {
        case REQUIRED:
          ((Decimal28DenseVector) vector).getMutator().setSafe(index, (Decimal28DenseHolder) holder);
          return;
        case OPTIONAL:
          if (((NullableDecimal28DenseHolder) holder).isSet == 1) {
            ((NullableDecimal28DenseVector) vector).getMutator().setSafe(index, (NullableDecimal28DenseHolder) holder);
          }
          return;
      }
    case DECIMAL38DENSE :
      switch (type.getMode()) {
        case REQUIRED:
          ((Decimal38DenseVector) vector).getMutator().setSafe(index, (Decimal38DenseHolder) holder);
          return;
        case OPTIONAL:
          if (((NullableDecimal38DenseHolder) holder).isSet == 1) {
            ((NullableDecimal38DenseVector) vector).getMutator().setSafe(index, (NullableDecimal38DenseHolder) holder);
          }
          return;
      }
    case DECIMAL38SPARSE :
      switch (type.getMode()) {
        case REQUIRED:
          ((Decimal38SparseVector) vector).getMutator().setSafe(index, (Decimal38SparseHolder) holder);
          return;
        case OPTIONAL:
          if (((NullableDecimal38SparseHolder) holder).isSet == 1) {
            ((NullableDecimal38SparseVector) vector).getMutator().setSafe(index, (NullableDecimal38SparseHolder) holder);
          }
          return;
      }
    case DECIMAL28SPARSE :
      switch (type.getMode()) {
        case REQUIRED:
          ((Decimal28SparseVector) vector).getMutator().setSafe(index, (Decimal28SparseHolder) holder);
          return;
        case OPTIONAL:
          if (((NullableDecimal28SparseHolder) holder).isSet == 1) {
            ((NullableDecimal28SparseVector) vector).getMutator().setSafe(index, (NullableDecimal28SparseHolder) holder);
          }
          return;
      }
    case VARBINARY :
      switch (type.getMode()) {
        case REQUIRED:
          ((VarBinaryVector) vector).getMutator().setSafe(index, (VarBinaryHolder) holder);
          return;
        case OPTIONAL:
          if (((NullableVarBinaryHolder) holder).isSet == 1) {
            ((NullableVarBinaryVector) vector).getMutator().setSafe(index, (NullableVarBinaryHolder) holder);
          }
          return;
      }
    case VARCHAR :
      switch (type.getMode()) {
        case REQUIRED:
          ((VarCharVector) vector).getMutator().setSafe(index, (VarCharHolder) holder);
          return;
        case OPTIONAL:
          if (((NullableVarCharHolder) holder).isSet == 1) {
            ((NullableVarCharVector) vector).getMutator().setSafe(index, (NullableVarCharHolder) holder);
          }
          return;
      }
    case VAR16CHAR :
      switch (type.getMode()) {
        case REQUIRED:
          ((Var16CharVector) vector).getMutator().setSafe(index, (Var16CharHolder) holder);
          return;
        case OPTIONAL:
          if (((NullableVar16CharHolder) holder).isSet == 1) {
            ((NullableVar16CharVector) vector).getMutator().setSafe(index, (NullableVar16CharHolder) holder);
          }
          return;
      }
    case BIT :
      switch (type.getMode()) {
        case REQUIRED:
          ((BitVector) vector).getMutator().setSafe(index, (BitHolder) holder);
          return;
        case OPTIONAL:
          if (((NullableBitHolder) holder).isSet == 1) {
            ((NullableBitVector) vector).getMutator().setSafe(index, (NullableBitHolder) holder);
          }
          return;
      }
    case GENERIC_OBJECT:
      ((ObjectVector) vector).getMutator().setSafe(index, (ObjectHolder) holder);
      return;
    default:
      throw new UnsupportedOperationException(buildErrorMessage("set value", type));
    }
  }

  public static void setValueSafe(ValueVector vector, int index, ValueHolder holder) {
    MajorType type = vector.getField().getType();

    switch(type.getMinorType()) {
      case TINYINT :
      switch (type.getMode()) {
        case REQUIRED:
          ((TinyIntVector) vector).getMutator().setSafe(index, (TinyIntHolder) holder);
          return;
        case OPTIONAL:
          if (((NullableTinyIntHolder) holder).isSet == 1) {
            ((NullableTinyIntVector) vector).getMutator().setSafe(index, (NullableTinyIntHolder) holder);
          } else {
            ((NullableTinyIntVector) vector).getMutator().isSafe(index);
          }
          return;
      }
      case UINT1 :
      switch (type.getMode()) {
        case REQUIRED:
          ((UInt1Vector) vector).getMutator().setSafe(index, (UInt1Holder) holder);
          return;
        case OPTIONAL:
          if (((NullableUInt1Holder) holder).isSet == 1) {
            ((NullableUInt1Vector) vector).getMutator().setSafe(index, (NullableUInt1Holder) holder);
          } else {
            ((NullableUInt1Vector) vector).getMutator().isSafe(index);
          }
          return;
      }
      case UINT2 :
      switch (type.getMode()) {
        case REQUIRED:
          ((UInt2Vector) vector).getMutator().setSafe(index, (UInt2Holder) holder);
          return;
        case OPTIONAL:
          if (((NullableUInt2Holder) holder).isSet == 1) {
            ((NullableUInt2Vector) vector).getMutator().setSafe(index, (NullableUInt2Holder) holder);
          } else {
            ((NullableUInt2Vector) vector).getMutator().isSafe(index);
          }
          return;
      }
      case SMALLINT :
      switch (type.getMode()) {
        case REQUIRED:
          ((SmallIntVector) vector).getMutator().setSafe(index, (SmallIntHolder) holder);
          return;
        case OPTIONAL:
          if (((NullableSmallIntHolder) holder).isSet == 1) {
            ((NullableSmallIntVector) vector).getMutator().setSafe(index, (NullableSmallIntHolder) holder);
          } else {
            ((NullableSmallIntVector) vector).getMutator().isSafe(index);
          }
          return;
      }
      case INT :
      switch (type.getMode()) {
        case REQUIRED:
          ((IntVector) vector).getMutator().setSafe(index, (IntHolder) holder);
          return;
        case OPTIONAL:
          if (((NullableIntHolder) holder).isSet == 1) {
            ((NullableIntVector) vector).getMutator().setSafe(index, (NullableIntHolder) holder);
          } else {
            ((NullableIntVector) vector).getMutator().isSafe(index);
          }
          return;
      }
      case UINT4 :
      switch (type.getMode()) {
        case REQUIRED:
          ((UInt4Vector) vector).getMutator().setSafe(index, (UInt4Holder) holder);
          return;
        case OPTIONAL:
          if (((NullableUInt4Holder) holder).isSet == 1) {
            ((NullableUInt4Vector) vector).getMutator().setSafe(index, (NullableUInt4Holder) holder);
          } else {
            ((NullableUInt4Vector) vector).getMutator().isSafe(index);
          }
          return;
      }
      case FLOAT4 :
      switch (type.getMode()) {
        case REQUIRED:
          ((Float4Vector) vector).getMutator().setSafe(index, (Float4Holder) holder);
          return;
        case OPTIONAL:
          if (((NullableFloat4Holder) holder).isSet == 1) {
            ((NullableFloat4Vector) vector).getMutator().setSafe(index, (NullableFloat4Holder) holder);
          } else {
            ((NullableFloat4Vector) vector).getMutator().isSafe(index);
          }
          return;
      }
      case TIME :
      switch (type.getMode()) {
        case REQUIRED:
          ((TimeVector) vector).getMutator().setSafe(index, (TimeHolder) holder);
          return;
        case OPTIONAL:
          if (((NullableTimeHolder) holder).isSet == 1) {
            ((NullableTimeVector) vector).getMutator().setSafe(index, (NullableTimeHolder) holder);
          } else {
            ((NullableTimeVector) vector).getMutator().isSafe(index);
          }
          return;
      }
      case INTERVALYEAR :
      switch (type.getMode()) {
        case REQUIRED:
          ((IntervalYearVector) vector).getMutator().setSafe(index, (IntervalYearHolder) holder);
          return;
        case OPTIONAL:
          if (((NullableIntervalYearHolder) holder).isSet == 1) {
            ((NullableIntervalYearVector) vector).getMutator().setSafe(index, (NullableIntervalYearHolder) holder);
          } else {
            ((NullableIntervalYearVector) vector).getMutator().isSafe(index);
          }
          return;
      }
      case DECIMAL9 :
      switch (type.getMode()) {
        case REQUIRED:
          ((Decimal9Vector) vector).getMutator().setSafe(index, (Decimal9Holder) holder);
          return;
        case OPTIONAL:
          if (((NullableDecimal9Holder) holder).isSet == 1) {
            ((NullableDecimal9Vector) vector).getMutator().setSafe(index, (NullableDecimal9Holder) holder);
          } else {
            ((NullableDecimal9Vector) vector).getMutator().isSafe(index);
          }
          return;
      }
      case BIGINT :
      switch (type.getMode()) {
        case REQUIRED:
          ((BigIntVector) vector).getMutator().setSafe(index, (BigIntHolder) holder);
          return;
        case OPTIONAL:
          if (((NullableBigIntHolder) holder).isSet == 1) {
            ((NullableBigIntVector) vector).getMutator().setSafe(index, (NullableBigIntHolder) holder);
          } else {
            ((NullableBigIntVector) vector).getMutator().isSafe(index);
          }
          return;
      }
      case UINT8 :
      switch (type.getMode()) {
        case REQUIRED:
          ((UInt8Vector) vector).getMutator().setSafe(index, (UInt8Holder) holder);
          return;
        case OPTIONAL:
          if (((NullableUInt8Holder) holder).isSet == 1) {
            ((NullableUInt8Vector) vector).getMutator().setSafe(index, (NullableUInt8Holder) holder);
          } else {
            ((NullableUInt8Vector) vector).getMutator().isSafe(index);
          }
          return;
      }
      case FLOAT8 :
      switch (type.getMode()) {
        case REQUIRED:
          ((Float8Vector) vector).getMutator().setSafe(index, (Float8Holder) holder);
          return;
        case OPTIONAL:
          if (((NullableFloat8Holder) holder).isSet == 1) {
            ((NullableFloat8Vector) vector).getMutator().setSafe(index, (NullableFloat8Holder) holder);
          } else {
            ((NullableFloat8Vector) vector).getMutator().isSafe(index);
          }
          return;
      }
      case DATE :
      switch (type.getMode()) {
        case REQUIRED:
          ((DateVector) vector).getMutator().setSafe(index, (DateHolder) holder);
          return;
        case OPTIONAL:
          if (((NullableDateHolder) holder).isSet == 1) {
            ((NullableDateVector) vector).getMutator().setSafe(index, (NullableDateHolder) holder);
          } else {
            ((NullableDateVector) vector).getMutator().isSafe(index);
          }
          return;
      }
      case TIMESTAMP :
      switch (type.getMode()) {
        case REQUIRED:
          ((TimeStampVector) vector).getMutator().setSafe(index, (TimeStampHolder) holder);
          return;
        case OPTIONAL:
          if (((NullableTimeStampHolder) holder).isSet == 1) {
            ((NullableTimeStampVector) vector).getMutator().setSafe(index, (NullableTimeStampHolder) holder);
          } else {
            ((NullableTimeStampVector) vector).getMutator().isSafe(index);
          }
          return;
      }
      case DECIMAL18 :
      switch (type.getMode()) {
        case REQUIRED:
          ((Decimal18Vector) vector).getMutator().setSafe(index, (Decimal18Holder) holder);
          return;
        case OPTIONAL:
          if (((NullableDecimal18Holder) holder).isSet == 1) {
            ((NullableDecimal18Vector) vector).getMutator().setSafe(index, (NullableDecimal18Holder) holder);
          } else {
            ((NullableDecimal18Vector) vector).getMutator().isSafe(index);
          }
          return;
      }
      case INTERVALDAY :
      switch (type.getMode()) {
        case REQUIRED:
          ((IntervalDayVector) vector).getMutator().setSafe(index, (IntervalDayHolder) holder);
          return;
        case OPTIONAL:
          if (((NullableIntervalDayHolder) holder).isSet == 1) {
            ((NullableIntervalDayVector) vector).getMutator().setSafe(index, (NullableIntervalDayHolder) holder);
          } else {
            ((NullableIntervalDayVector) vector).getMutator().isSafe(index);
          }
          return;
      }
      case INTERVAL :
      switch (type.getMode()) {
        case REQUIRED:
          ((IntervalVector) vector).getMutator().setSafe(index, (IntervalHolder) holder);
          return;
        case OPTIONAL:
          if (((NullableIntervalHolder) holder).isSet == 1) {
            ((NullableIntervalVector) vector).getMutator().setSafe(index, (NullableIntervalHolder) holder);
          } else {
            ((NullableIntervalVector) vector).getMutator().isSafe(index);
          }
          return;
      }
      case DECIMAL28DENSE :
      switch (type.getMode()) {
        case REQUIRED:
          ((Decimal28DenseVector) vector).getMutator().setSafe(index, (Decimal28DenseHolder) holder);
          return;
        case OPTIONAL:
          if (((NullableDecimal28DenseHolder) holder).isSet == 1) {
            ((NullableDecimal28DenseVector) vector).getMutator().setSafe(index, (NullableDecimal28DenseHolder) holder);
          } else {
            ((NullableDecimal28DenseVector) vector).getMutator().isSafe(index);
          }
          return;
      }
      case DECIMAL38DENSE :
      switch (type.getMode()) {
        case REQUIRED:
          ((Decimal38DenseVector) vector).getMutator().setSafe(index, (Decimal38DenseHolder) holder);
          return;
        case OPTIONAL:
          if (((NullableDecimal38DenseHolder) holder).isSet == 1) {
            ((NullableDecimal38DenseVector) vector).getMutator().setSafe(index, (NullableDecimal38DenseHolder) holder);
          } else {
            ((NullableDecimal38DenseVector) vector).getMutator().isSafe(index);
          }
          return;
      }
      case DECIMAL38SPARSE :
      switch (type.getMode()) {
        case REQUIRED:
          ((Decimal38SparseVector) vector).getMutator().setSafe(index, (Decimal38SparseHolder) holder);
          return;
        case OPTIONAL:
          if (((NullableDecimal38SparseHolder) holder).isSet == 1) {
            ((NullableDecimal38SparseVector) vector).getMutator().setSafe(index, (NullableDecimal38SparseHolder) holder);
          } else {
            ((NullableDecimal38SparseVector) vector).getMutator().isSafe(index);
          }
          return;
      }
      case DECIMAL28SPARSE :
      switch (type.getMode()) {
        case REQUIRED:
          ((Decimal28SparseVector) vector).getMutator().setSafe(index, (Decimal28SparseHolder) holder);
          return;
        case OPTIONAL:
          if (((NullableDecimal28SparseHolder) holder).isSet == 1) {
            ((NullableDecimal28SparseVector) vector).getMutator().setSafe(index, (NullableDecimal28SparseHolder) holder);
          } else {
            ((NullableDecimal28SparseVector) vector).getMutator().isSafe(index);
          }
          return;
      }
      case VARBINARY :
      switch (type.getMode()) {
        case REQUIRED:
          ((VarBinaryVector) vector).getMutator().setSafe(index, (VarBinaryHolder) holder);
          return;
        case OPTIONAL:
          if (((NullableVarBinaryHolder) holder).isSet == 1) {
            ((NullableVarBinaryVector) vector).getMutator().setSafe(index, (NullableVarBinaryHolder) holder);
          } else {
            ((NullableVarBinaryVector) vector).getMutator().isSafe(index);
          }
          return;
      }
      case VARCHAR :
      switch (type.getMode()) {
        case REQUIRED:
          ((VarCharVector) vector).getMutator().setSafe(index, (VarCharHolder) holder);
          return;
        case OPTIONAL:
          if (((NullableVarCharHolder) holder).isSet == 1) {
            ((NullableVarCharVector) vector).getMutator().setSafe(index, (NullableVarCharHolder) holder);
          } else {
            ((NullableVarCharVector) vector).getMutator().isSafe(index);
          }
          return;
      }
      case VAR16CHAR :
      switch (type.getMode()) {
        case REQUIRED:
          ((Var16CharVector) vector).getMutator().setSafe(index, (Var16CharHolder) holder);
          return;
        case OPTIONAL:
          if (((NullableVar16CharHolder) holder).isSet == 1) {
            ((NullableVar16CharVector) vector).getMutator().setSafe(index, (NullableVar16CharHolder) holder);
          } else {
            ((NullableVar16CharVector) vector).getMutator().isSafe(index);
          }
          return;
      }
      case BIT :
      switch (type.getMode()) {
        case REQUIRED:
          ((BitVector) vector).getMutator().setSafe(index, (BitHolder) holder);
          return;
        case OPTIONAL:
          if (((NullableBitHolder) holder).isSet == 1) {
            ((NullableBitVector) vector).getMutator().setSafe(index, (NullableBitHolder) holder);
          } else {
            ((NullableBitVector) vector).getMutator().isSafe(index);
          }
          return;
      }
      case GENERIC_OBJECT:
        ((ObjectVector) vector).getMutator().setSafe(index, (ObjectHolder) holder);
      default:
        throw new UnsupportedOperationException(buildErrorMessage("set value safe", type));
    }
  }

  public static boolean compareValues(ValueVector v1, int v1index, ValueVector v2, int v2index) {
    MajorType type1 = v1.getField().getType();
    MajorType type2 = v2.getField().getType();

    if (type1.getMinorType() != type2.getMinorType()) {
      return false;
    }

    switch(type1.getMinorType()) {
    case TINYINT :
      if ( ((TinyIntVector) v1).getAccessor().get(v1index) == 
           ((TinyIntVector) v2).getAccessor().get(v2index) ) 
        return true;
      break;
    case UINT1 :
      if ( ((UInt1Vector) v1).getAccessor().get(v1index) == 
           ((UInt1Vector) v2).getAccessor().get(v2index) ) 
        return true;
      break;
    case UINT2 :
      if ( ((UInt2Vector) v1).getAccessor().get(v1index) == 
           ((UInt2Vector) v2).getAccessor().get(v2index) ) 
        return true;
      break;
    case SMALLINT :
      if ( ((SmallIntVector) v1).getAccessor().get(v1index) == 
           ((SmallIntVector) v2).getAccessor().get(v2index) ) 
        return true;
      break;
    case INT :
      if ( ((IntVector) v1).getAccessor().get(v1index) == 
           ((IntVector) v2).getAccessor().get(v2index) ) 
        return true;
      break;
    case UINT4 :
      if ( ((UInt4Vector) v1).getAccessor().get(v1index) == 
           ((UInt4Vector) v2).getAccessor().get(v2index) ) 
        return true;
      break;
    case FLOAT4 :
      if ( ((Float4Vector) v1).getAccessor().get(v1index) == 
           ((Float4Vector) v2).getAccessor().get(v2index) ) 
        return true;
      break;
    case TIME :
      if ( ((TimeVector) v1).getAccessor().get(v1index) == 
           ((TimeVector) v2).getAccessor().get(v2index) ) 
        return true;
      break;
    case INTERVALYEAR :
      if ( ((IntervalYearVector) v1).getAccessor().get(v1index) == 
           ((IntervalYearVector) v2).getAccessor().get(v2index) ) 
        return true;
      break;
    case DECIMAL9 :
      if ( ((Decimal9Vector) v1).getAccessor().get(v1index) == 
           ((Decimal9Vector) v2).getAccessor().get(v2index) ) 
        return true;
      break;
    case BIGINT :
      if ( ((BigIntVector) v1).getAccessor().get(v1index) == 
           ((BigIntVector) v2).getAccessor().get(v2index) ) 
        return true;
      break;
    case UINT8 :
      if ( ((UInt8Vector) v1).getAccessor().get(v1index) == 
           ((UInt8Vector) v2).getAccessor().get(v2index) ) 
        return true;
      break;
    case FLOAT8 :
      if ( ((Float8Vector) v1).getAccessor().get(v1index) == 
           ((Float8Vector) v2).getAccessor().get(v2index) ) 
        return true;
      break;
    case DATE :
      if ( ((DateVector) v1).getAccessor().get(v1index) == 
           ((DateVector) v2).getAccessor().get(v2index) ) 
        return true;
      break;
    case TIMESTAMP :
      if ( ((TimeStampVector) v1).getAccessor().get(v1index) == 
           ((TimeStampVector) v2).getAccessor().get(v2index) ) 
        return true;
      break;
    case DECIMAL18 :
      if ( ((Decimal18Vector) v1).getAccessor().get(v1index) == 
           ((Decimal18Vector) v2).getAccessor().get(v2index) ) 
        return true;
      break;
    case INTERVALDAY :
      if ( ((IntervalDayVector) v1).getAccessor().get(v1index) == 
           ((IntervalDayVector) v2).getAccessor().get(v2index) ) 
        return true;
      break;
    case INTERVAL :
      if ( ((IntervalVector) v1).getAccessor().get(v1index) == 
           ((IntervalVector) v2).getAccessor().get(v2index) ) 
        return true;
      break;
    case DECIMAL28DENSE :
      if ( ((Decimal28DenseVector) v1).getAccessor().get(v1index) == 
           ((Decimal28DenseVector) v2).getAccessor().get(v2index) ) 
        return true;
      break;
    case DECIMAL38DENSE :
      if ( ((Decimal38DenseVector) v1).getAccessor().get(v1index) == 
           ((Decimal38DenseVector) v2).getAccessor().get(v2index) ) 
        return true;
      break;
    case DECIMAL38SPARSE :
      if ( ((Decimal38SparseVector) v1).getAccessor().get(v1index) == 
           ((Decimal38SparseVector) v2).getAccessor().get(v2index) ) 
        return true;
      break;
    case DECIMAL28SPARSE :
      if ( ((Decimal28SparseVector) v1).getAccessor().get(v1index) == 
           ((Decimal28SparseVector) v2).getAccessor().get(v2index) ) 
        return true;
      break;
    case VARBINARY :
      if ( ((VarBinaryVector) v1).getAccessor().get(v1index) == 
           ((VarBinaryVector) v2).getAccessor().get(v2index) ) 
        return true;
      break;
    case VARCHAR :
      if ( ((VarCharVector) v1).getAccessor().get(v1index) == 
           ((VarCharVector) v2).getAccessor().get(v2index) ) 
        return true;
      break;
    case VAR16CHAR :
      if ( ((Var16CharVector) v1).getAccessor().get(v1index) == 
           ((Var16CharVector) v2).getAccessor().get(v2index) ) 
        return true;
      break;
    case BIT :
      if ( ((BitVector) v1).getAccessor().get(v1index) == 
           ((BitVector) v2).getAccessor().get(v2index) ) 
        return true;
      break;
    default:
      break;
    }
    return false;
  }

  /**
   *  Create a ValueHolder of MajorType.
   * @param type
   * @return
   */
  public static ValueHolder createValueHolder(MajorType type) {
    switch(type.getMinorType()) {
      case TINYINT :

        switch (type.getMode()) {
          case REQUIRED:
            return new TinyIntHolder();
          case OPTIONAL:
            return new NullableTinyIntHolder();
          case REPEATED:
            return new RepeatedTinyIntHolder();
        }
      case UINT1 :

        switch (type.getMode()) {
          case REQUIRED:
            return new UInt1Holder();
          case OPTIONAL:
            return new NullableUInt1Holder();
          case REPEATED:
            return new RepeatedUInt1Holder();
        }
      case UINT2 :

        switch (type.getMode()) {
          case REQUIRED:
            return new UInt2Holder();
          case OPTIONAL:
            return new NullableUInt2Holder();
          case REPEATED:
            return new RepeatedUInt2Holder();
        }
      case SMALLINT :

        switch (type.getMode()) {
          case REQUIRED:
            return new SmallIntHolder();
          case OPTIONAL:
            return new NullableSmallIntHolder();
          case REPEATED:
            return new RepeatedSmallIntHolder();
        }
      case INT :

        switch (type.getMode()) {
          case REQUIRED:
            return new IntHolder();
          case OPTIONAL:
            return new NullableIntHolder();
          case REPEATED:
            return new RepeatedIntHolder();
        }
      case UINT4 :

        switch (type.getMode()) {
          case REQUIRED:
            return new UInt4Holder();
          case OPTIONAL:
            return new NullableUInt4Holder();
          case REPEATED:
            return new RepeatedUInt4Holder();
        }
      case FLOAT4 :

        switch (type.getMode()) {
          case REQUIRED:
            return new Float4Holder();
          case OPTIONAL:
            return new NullableFloat4Holder();
          case REPEATED:
            return new RepeatedFloat4Holder();
        }
      case TIME :

        switch (type.getMode()) {
          case REQUIRED:
            return new TimeHolder();
          case OPTIONAL:
            return new NullableTimeHolder();
          case REPEATED:
            return new RepeatedTimeHolder();
        }
      case INTERVALYEAR :

        switch (type.getMode()) {
          case REQUIRED:
            return new IntervalYearHolder();
          case OPTIONAL:
            return new NullableIntervalYearHolder();
          case REPEATED:
            return new RepeatedIntervalYearHolder();
        }
      case DECIMAL9 :

        switch (type.getMode()) {
          case REQUIRED:
            return new Decimal9Holder();
          case OPTIONAL:
            return new NullableDecimal9Holder();
          case REPEATED:
            return new RepeatedDecimal9Holder();
        }
      case BIGINT :

        switch (type.getMode()) {
          case REQUIRED:
            return new BigIntHolder();
          case OPTIONAL:
            return new NullableBigIntHolder();
          case REPEATED:
            return new RepeatedBigIntHolder();
        }
      case UINT8 :

        switch (type.getMode()) {
          case REQUIRED:
            return new UInt8Holder();
          case OPTIONAL:
            return new NullableUInt8Holder();
          case REPEATED:
            return new RepeatedUInt8Holder();
        }
      case FLOAT8 :

        switch (type.getMode()) {
          case REQUIRED:
            return new Float8Holder();
          case OPTIONAL:
            return new NullableFloat8Holder();
          case REPEATED:
            return new RepeatedFloat8Holder();
        }
      case DATE :

        switch (type.getMode()) {
          case REQUIRED:
            return new DateHolder();
          case OPTIONAL:
            return new NullableDateHolder();
          case REPEATED:
            return new RepeatedDateHolder();
        }
      case TIMESTAMP :

        switch (type.getMode()) {
          case REQUIRED:
            return new TimeStampHolder();
          case OPTIONAL:
            return new NullableTimeStampHolder();
          case REPEATED:
            return new RepeatedTimeStampHolder();
        }
      case DECIMAL18 :

        switch (type.getMode()) {
          case REQUIRED:
            return new Decimal18Holder();
          case OPTIONAL:
            return new NullableDecimal18Holder();
          case REPEATED:
            return new RepeatedDecimal18Holder();
        }
      case INTERVALDAY :

        switch (type.getMode()) {
          case REQUIRED:
            return new IntervalDayHolder();
          case OPTIONAL:
            return new NullableIntervalDayHolder();
          case REPEATED:
            return new RepeatedIntervalDayHolder();
        }
      case INTERVAL :

        switch (type.getMode()) {
          case REQUIRED:
            return new IntervalHolder();
          case OPTIONAL:
            return new NullableIntervalHolder();
          case REPEATED:
            return new RepeatedIntervalHolder();
        }
      case DECIMAL28DENSE :

        switch (type.getMode()) {
          case REQUIRED:
            return new Decimal28DenseHolder();
          case OPTIONAL:
            return new NullableDecimal28DenseHolder();
          case REPEATED:
            return new RepeatedDecimal28DenseHolder();
        }
      case DECIMAL38DENSE :

        switch (type.getMode()) {
          case REQUIRED:
            return new Decimal38DenseHolder();
          case OPTIONAL:
            return new NullableDecimal38DenseHolder();
          case REPEATED:
            return new RepeatedDecimal38DenseHolder();
        }
      case DECIMAL38SPARSE :

        switch (type.getMode()) {
          case REQUIRED:
            return new Decimal38SparseHolder();
          case OPTIONAL:
            return new NullableDecimal38SparseHolder();
          case REPEATED:
            return new RepeatedDecimal38SparseHolder();
        }
      case DECIMAL28SPARSE :

        switch (type.getMode()) {
          case REQUIRED:
            return new Decimal28SparseHolder();
          case OPTIONAL:
            return new NullableDecimal28SparseHolder();
          case REPEATED:
            return new RepeatedDecimal28SparseHolder();
        }
      case VARBINARY :

        switch (type.getMode()) {
          case REQUIRED:
            return new VarBinaryHolder();
          case OPTIONAL:
            return new NullableVarBinaryHolder();
          case REPEATED:
            return new RepeatedVarBinaryHolder();
        }
      case VARCHAR :

        switch (type.getMode()) {
          case REQUIRED:
            return new VarCharHolder();
          case OPTIONAL:
            return new NullableVarCharHolder();
          case REPEATED:
            return new RepeatedVarCharHolder();
        }
      case VAR16CHAR :

        switch (type.getMode()) {
          case REQUIRED:
            return new Var16CharHolder();
          case OPTIONAL:
            return new NullableVar16CharHolder();
          case REPEATED:
            return new RepeatedVar16CharHolder();
        }
      case BIT :

        switch (type.getMode()) {
          case REQUIRED:
            return new BitHolder();
          case OPTIONAL:
            return new NullableBitHolder();
          case REPEATED:
            return new RepeatedBitHolder();
        }
      case GENERIC_OBJECT:
        return new ObjectHolder();
      default:
        throw new UnsupportedOperationException(buildErrorMessage("create value holder", type));
    }
  }

  public static boolean isNull(ValueHolder holder) {
    MajorType type = getValueHolderType(holder);

    switch(type.getMinorType()) {
      case TINYINT :

      switch (type.getMode()) {
        case REQUIRED:
          return true;
        case OPTIONAL:
          return ((NullableTinyIntHolder) holder).isSet == 0;
        case REPEATED:
          return true;
      }
      case UINT1 :

      switch (type.getMode()) {
        case REQUIRED:
          return true;
        case OPTIONAL:
          return ((NullableUInt1Holder) holder).isSet == 0;
        case REPEATED:
          return true;
      }
      case UINT2 :

      switch (type.getMode()) {
        case REQUIRED:
          return true;
        case OPTIONAL:
          return ((NullableUInt2Holder) holder).isSet == 0;
        case REPEATED:
          return true;
      }
      case SMALLINT :

      switch (type.getMode()) {
        case REQUIRED:
          return true;
        case OPTIONAL:
          return ((NullableSmallIntHolder) holder).isSet == 0;
        case REPEATED:
          return true;
      }
      case INT :

      switch (type.getMode()) {
        case REQUIRED:
          return true;
        case OPTIONAL:
          return ((NullableIntHolder) holder).isSet == 0;
        case REPEATED:
          return true;
      }
      case UINT4 :

      switch (type.getMode()) {
        case REQUIRED:
          return true;
        case OPTIONAL:
          return ((NullableUInt4Holder) holder).isSet == 0;
        case REPEATED:
          return true;
      }
      case FLOAT4 :

      switch (type.getMode()) {
        case REQUIRED:
          return true;
        case OPTIONAL:
          return ((NullableFloat4Holder) holder).isSet == 0;
        case REPEATED:
          return true;
      }
      case TIME :

      switch (type.getMode()) {
        case REQUIRED:
          return true;
        case OPTIONAL:
          return ((NullableTimeHolder) holder).isSet == 0;
        case REPEATED:
          return true;
      }
      case INTERVALYEAR :

      switch (type.getMode()) {
        case REQUIRED:
          return true;
        case OPTIONAL:
          return ((NullableIntervalYearHolder) holder).isSet == 0;
        case REPEATED:
          return true;
      }
      case DECIMAL9 :

      switch (type.getMode()) {
        case REQUIRED:
          return true;
        case OPTIONAL:
          return ((NullableDecimal9Holder) holder).isSet == 0;
        case REPEATED:
          return true;
      }
      case BIGINT :

      switch (type.getMode()) {
        case REQUIRED:
          return true;
        case OPTIONAL:
          return ((NullableBigIntHolder) holder).isSet == 0;
        case REPEATED:
          return true;
      }
      case UINT8 :

      switch (type.getMode()) {
        case REQUIRED:
          return true;
        case OPTIONAL:
          return ((NullableUInt8Holder) holder).isSet == 0;
        case REPEATED:
          return true;
      }
      case FLOAT8 :

      switch (type.getMode()) {
        case REQUIRED:
          return true;
        case OPTIONAL:
          return ((NullableFloat8Holder) holder).isSet == 0;
        case REPEATED:
          return true;
      }
      case DATE :

      switch (type.getMode()) {
        case REQUIRED:
          return true;
        case OPTIONAL:
          return ((NullableDateHolder) holder).isSet == 0;
        case REPEATED:
          return true;
      }
      case TIMESTAMP :

      switch (type.getMode()) {
        case REQUIRED:
          return true;
        case OPTIONAL:
          return ((NullableTimeStampHolder) holder).isSet == 0;
        case REPEATED:
          return true;
      }
      case DECIMAL18 :

      switch (type.getMode()) {
        case REQUIRED:
          return true;
        case OPTIONAL:
          return ((NullableDecimal18Holder) holder).isSet == 0;
        case REPEATED:
          return true;
      }
      case INTERVALDAY :

      switch (type.getMode()) {
        case REQUIRED:
          return true;
        case OPTIONAL:
          return ((NullableIntervalDayHolder) holder).isSet == 0;
        case REPEATED:
          return true;
      }
      case INTERVAL :

      switch (type.getMode()) {
        case REQUIRED:
          return true;
        case OPTIONAL:
          return ((NullableIntervalHolder) holder).isSet == 0;
        case REPEATED:
          return true;
      }
      case DECIMAL28DENSE :

      switch (type.getMode()) {
        case REQUIRED:
          return true;
        case OPTIONAL:
          return ((NullableDecimal28DenseHolder) holder).isSet == 0;
        case REPEATED:
          return true;
      }
      case DECIMAL38DENSE :

      switch (type.getMode()) {
        case REQUIRED:
          return true;
        case OPTIONAL:
          return ((NullableDecimal38DenseHolder) holder).isSet == 0;
        case REPEATED:
          return true;
      }
      case DECIMAL38SPARSE :

      switch (type.getMode()) {
        case REQUIRED:
          return true;
        case OPTIONAL:
          return ((NullableDecimal38SparseHolder) holder).isSet == 0;
        case REPEATED:
          return true;
      }
      case DECIMAL28SPARSE :

      switch (type.getMode()) {
        case REQUIRED:
          return true;
        case OPTIONAL:
          return ((NullableDecimal28SparseHolder) holder).isSet == 0;
        case REPEATED:
          return true;
      }
      case VARBINARY :

      switch (type.getMode()) {
        case REQUIRED:
          return true;
        case OPTIONAL:
          return ((NullableVarBinaryHolder) holder).isSet == 0;
        case REPEATED:
          return true;
      }
      case VARCHAR :

      switch (type.getMode()) {
        case REQUIRED:
          return true;
        case OPTIONAL:
          return ((NullableVarCharHolder) holder).isSet == 0;
        case REPEATED:
          return true;
      }
      case VAR16CHAR :

      switch (type.getMode()) {
        case REQUIRED:
          return true;
        case OPTIONAL:
          return ((NullableVar16CharHolder) holder).isSet == 0;
        case REPEATED:
          return true;
      }
      case BIT :

      switch (type.getMode()) {
        case REQUIRED:
          return true;
        case OPTIONAL:
          return ((NullableBitHolder) holder).isSet == 0;
        case REPEATED:
          return true;
      }
      default:
        throw new UnsupportedOperationException(buildErrorMessage("check is null", type));
    }
  }

  public static ValueHolder deNullify(ValueHolder holder) {
    MajorType type = getValueHolderType(holder);

    switch(type.getMinorType()) {
      case TINYINT :

        switch (type.getMode()) {
          case REQUIRED:
            return holder;
          case OPTIONAL:
            if( ((NullableTinyIntHolder) holder).isSet == 1) {
              TinyIntHolder newHolder = new TinyIntHolder();

              newHolder.value = ((NullableTinyIntHolder) holder).value;

              return newHolder;
            } else {
              throw new UnsupportedOperationException("You can not convert a null value into a non-null value!");
            }
          case REPEATED:
            return holder;
        }
      case UINT1 :

        switch (type.getMode()) {
          case REQUIRED:
            return holder;
          case OPTIONAL:
            if( ((NullableUInt1Holder) holder).isSet == 1) {
              UInt1Holder newHolder = new UInt1Holder();

              newHolder.value = ((NullableUInt1Holder) holder).value;

              return newHolder;
            } else {
              throw new UnsupportedOperationException("You can not convert a null value into a non-null value!");
            }
          case REPEATED:
            return holder;
        }
      case UINT2 :

        switch (type.getMode()) {
          case REQUIRED:
            return holder;
          case OPTIONAL:
            if( ((NullableUInt2Holder) holder).isSet == 1) {
              UInt2Holder newHolder = new UInt2Holder();

              newHolder.value = ((NullableUInt2Holder) holder).value;

              return newHolder;
            } else {
              throw new UnsupportedOperationException("You can not convert a null value into a non-null value!");
            }
          case REPEATED:
            return holder;
        }
      case SMALLINT :

        switch (type.getMode()) {
          case REQUIRED:
            return holder;
          case OPTIONAL:
            if( ((NullableSmallIntHolder) holder).isSet == 1) {
              SmallIntHolder newHolder = new SmallIntHolder();

              newHolder.value = ((NullableSmallIntHolder) holder).value;

              return newHolder;
            } else {
              throw new UnsupportedOperationException("You can not convert a null value into a non-null value!");
            }
          case REPEATED:
            return holder;
        }
      case INT :

        switch (type.getMode()) {
          case REQUIRED:
            return holder;
          case OPTIONAL:
            if( ((NullableIntHolder) holder).isSet == 1) {
              IntHolder newHolder = new IntHolder();

              newHolder.value = ((NullableIntHolder) holder).value;

              return newHolder;
            } else {
              throw new UnsupportedOperationException("You can not convert a null value into a non-null value!");
            }
          case REPEATED:
            return holder;
        }
      case UINT4 :

        switch (type.getMode()) {
          case REQUIRED:
            return holder;
          case OPTIONAL:
            if( ((NullableUInt4Holder) holder).isSet == 1) {
              UInt4Holder newHolder = new UInt4Holder();

              newHolder.value = ((NullableUInt4Holder) holder).value;

              return newHolder;
            } else {
              throw new UnsupportedOperationException("You can not convert a null value into a non-null value!");
            }
          case REPEATED:
            return holder;
        }
      case FLOAT4 :

        switch (type.getMode()) {
          case REQUIRED:
            return holder;
          case OPTIONAL:
            if( ((NullableFloat4Holder) holder).isSet == 1) {
              Float4Holder newHolder = new Float4Holder();

              newHolder.value = ((NullableFloat4Holder) holder).value;

              return newHolder;
            } else {
              throw new UnsupportedOperationException("You can not convert a null value into a non-null value!");
            }
          case REPEATED:
            return holder;
        }
      case TIME :

        switch (type.getMode()) {
          case REQUIRED:
            return holder;
          case OPTIONAL:
            if( ((NullableTimeHolder) holder).isSet == 1) {
              TimeHolder newHolder = new TimeHolder();

              newHolder.value = ((NullableTimeHolder) holder).value;

              return newHolder;
            } else {
              throw new UnsupportedOperationException("You can not convert a null value into a non-null value!");
            }
          case REPEATED:
            return holder;
        }
      case INTERVALYEAR :

        switch (type.getMode()) {
          case REQUIRED:
            return holder;
          case OPTIONAL:
            if( ((NullableIntervalYearHolder) holder).isSet == 1) {
              IntervalYearHolder newHolder = new IntervalYearHolder();

              newHolder.value = ((NullableIntervalYearHolder) holder).value;

              return newHolder;
            } else {
              throw new UnsupportedOperationException("You can not convert a null value into a non-null value!");
            }
          case REPEATED:
            return holder;
        }
      case DECIMAL9 :

        switch (type.getMode()) {
          case REQUIRED:
            return holder;
          case OPTIONAL:
            if( ((NullableDecimal9Holder) holder).isSet == 1) {
              Decimal9Holder newHolder = new Decimal9Holder();

              newHolder.value = ((NullableDecimal9Holder) holder).value;
              newHolder.scale = ((NullableDecimal9Holder) holder).scale;
              newHolder.precision = ((NullableDecimal9Holder) holder).precision;

              return newHolder;
            } else {
              throw new UnsupportedOperationException("You can not convert a null value into a non-null value!");
            }
          case REPEATED:
            return holder;
        }
      case BIGINT :

        switch (type.getMode()) {
          case REQUIRED:
            return holder;
          case OPTIONAL:
            if( ((NullableBigIntHolder) holder).isSet == 1) {
              BigIntHolder newHolder = new BigIntHolder();

              newHolder.value = ((NullableBigIntHolder) holder).value;

              return newHolder;
            } else {
              throw new UnsupportedOperationException("You can not convert a null value into a non-null value!");
            }
          case REPEATED:
            return holder;
        }
      case UINT8 :

        switch (type.getMode()) {
          case REQUIRED:
            return holder;
          case OPTIONAL:
            if( ((NullableUInt8Holder) holder).isSet == 1) {
              UInt8Holder newHolder = new UInt8Holder();

              newHolder.value = ((NullableUInt8Holder) holder).value;

              return newHolder;
            } else {
              throw new UnsupportedOperationException("You can not convert a null value into a non-null value!");
            }
          case REPEATED:
            return holder;
        }
      case FLOAT8 :

        switch (type.getMode()) {
          case REQUIRED:
            return holder;
          case OPTIONAL:
            if( ((NullableFloat8Holder) holder).isSet == 1) {
              Float8Holder newHolder = new Float8Holder();

              newHolder.value = ((NullableFloat8Holder) holder).value;

              return newHolder;
            } else {
              throw new UnsupportedOperationException("You can not convert a null value into a non-null value!");
            }
          case REPEATED:
            return holder;
        }
      case DATE :

        switch (type.getMode()) {
          case REQUIRED:
            return holder;
          case OPTIONAL:
            if( ((NullableDateHolder) holder).isSet == 1) {
              DateHolder newHolder = new DateHolder();

              newHolder.value = ((NullableDateHolder) holder).value;

              return newHolder;
            } else {
              throw new UnsupportedOperationException("You can not convert a null value into a non-null value!");
            }
          case REPEATED:
            return holder;
        }
      case TIMESTAMP :

        switch (type.getMode()) {
          case REQUIRED:
            return holder;
          case OPTIONAL:
            if( ((NullableTimeStampHolder) holder).isSet == 1) {
              TimeStampHolder newHolder = new TimeStampHolder();

              newHolder.value = ((NullableTimeStampHolder) holder).value;

              return newHolder;
            } else {
              throw new UnsupportedOperationException("You can not convert a null value into a non-null value!");
            }
          case REPEATED:
            return holder;
        }
      case DECIMAL18 :

        switch (type.getMode()) {
          case REQUIRED:
            return holder;
          case OPTIONAL:
            if( ((NullableDecimal18Holder) holder).isSet == 1) {
              Decimal18Holder newHolder = new Decimal18Holder();

              newHolder.value = ((NullableDecimal18Holder) holder).value;
              newHolder.scale = ((NullableDecimal18Holder) holder).scale;
              newHolder.precision = ((NullableDecimal18Holder) holder).precision;

              return newHolder;
            } else {
              throw new UnsupportedOperationException("You can not convert a null value into a non-null value!");
            }
          case REPEATED:
            return holder;
        }
      case INTERVALDAY :

        switch (type.getMode()) {
          case REQUIRED:
            return holder;
          case OPTIONAL:
            if( ((NullableIntervalDayHolder) holder).isSet == 1) {
              IntervalDayHolder newHolder = new IntervalDayHolder();

              newHolder.days = ((NullableIntervalDayHolder) holder).days;
              newHolder.milliseconds = ((NullableIntervalDayHolder) holder).milliseconds;

              return newHolder;
            } else {
              throw new UnsupportedOperationException("You can not convert a null value into a non-null value!");
            }
          case REPEATED:
            return holder;
        }
      case INTERVAL :

        switch (type.getMode()) {
          case REQUIRED:
            return holder;
          case OPTIONAL:
            if( ((NullableIntervalHolder) holder).isSet == 1) {
              IntervalHolder newHolder = new IntervalHolder();

              newHolder.months = ((NullableIntervalHolder) holder).months;
              newHolder.days = ((NullableIntervalHolder) holder).days;
              newHolder.milliseconds = ((NullableIntervalHolder) holder).milliseconds;

              return newHolder;
            } else {
              throw new UnsupportedOperationException("You can not convert a null value into a non-null value!");
            }
          case REPEATED:
            return holder;
        }
      case DECIMAL28DENSE :

        switch (type.getMode()) {
          case REQUIRED:
            return holder;
          case OPTIONAL:
            if( ((NullableDecimal28DenseHolder) holder).isSet == 1) {
              Decimal28DenseHolder newHolder = new Decimal28DenseHolder();

              newHolder.start = ((NullableDecimal28DenseHolder) holder).start;
              newHolder.buffer = ((NullableDecimal28DenseHolder) holder).buffer;
              newHolder.scale = ((NullableDecimal28DenseHolder) holder).scale;
              newHolder.precision = ((NullableDecimal28DenseHolder) holder).precision;

              return newHolder;
            } else {
              throw new UnsupportedOperationException("You can not convert a null value into a non-null value!");
            }
          case REPEATED:
            return holder;
        }
      case DECIMAL38DENSE :

        switch (type.getMode()) {
          case REQUIRED:
            return holder;
          case OPTIONAL:
            if( ((NullableDecimal38DenseHolder) holder).isSet == 1) {
              Decimal38DenseHolder newHolder = new Decimal38DenseHolder();

              newHolder.start = ((NullableDecimal38DenseHolder) holder).start;
              newHolder.buffer = ((NullableDecimal38DenseHolder) holder).buffer;
              newHolder.scale = ((NullableDecimal38DenseHolder) holder).scale;
              newHolder.precision = ((NullableDecimal38DenseHolder) holder).precision;

              return newHolder;
            } else {
              throw new UnsupportedOperationException("You can not convert a null value into a non-null value!");
            }
          case REPEATED:
            return holder;
        }
      case DECIMAL38SPARSE :

        switch (type.getMode()) {
          case REQUIRED:
            return holder;
          case OPTIONAL:
            if( ((NullableDecimal38SparseHolder) holder).isSet == 1) {
              Decimal38SparseHolder newHolder = new Decimal38SparseHolder();

              newHolder.start = ((NullableDecimal38SparseHolder) holder).start;
              newHolder.buffer = ((NullableDecimal38SparseHolder) holder).buffer;
              newHolder.scale = ((NullableDecimal38SparseHolder) holder).scale;
              newHolder.precision = ((NullableDecimal38SparseHolder) holder).precision;

              return newHolder;
            } else {
              throw new UnsupportedOperationException("You can not convert a null value into a non-null value!");
            }
          case REPEATED:
            return holder;
        }
      case DECIMAL28SPARSE :

        switch (type.getMode()) {
          case REQUIRED:
            return holder;
          case OPTIONAL:
            if( ((NullableDecimal28SparseHolder) holder).isSet == 1) {
              Decimal28SparseHolder newHolder = new Decimal28SparseHolder();

              newHolder.start = ((NullableDecimal28SparseHolder) holder).start;
              newHolder.buffer = ((NullableDecimal28SparseHolder) holder).buffer;
              newHolder.scale = ((NullableDecimal28SparseHolder) holder).scale;
              newHolder.precision = ((NullableDecimal28SparseHolder) holder).precision;

              return newHolder;
            } else {
              throw new UnsupportedOperationException("You can not convert a null value into a non-null value!");
            }
          case REPEATED:
            return holder;
        }
      case VARBINARY :

        switch (type.getMode()) {
          case REQUIRED:
            return holder;
          case OPTIONAL:
            if( ((NullableVarBinaryHolder) holder).isSet == 1) {
              VarBinaryHolder newHolder = new VarBinaryHolder();

              newHolder.start = ((NullableVarBinaryHolder) holder).start;
              newHolder.end = ((NullableVarBinaryHolder) holder).end;
              newHolder.buffer = ((NullableVarBinaryHolder) holder).buffer;

              return newHolder;
            } else {
              throw new UnsupportedOperationException("You can not convert a null value into a non-null value!");
            }
          case REPEATED:
            return holder;
        }
      case VARCHAR :

        switch (type.getMode()) {
          case REQUIRED:
            return holder;
          case OPTIONAL:
            if( ((NullableVarCharHolder) holder).isSet == 1) {
              VarCharHolder newHolder = new VarCharHolder();

              newHolder.start = ((NullableVarCharHolder) holder).start;
              newHolder.end = ((NullableVarCharHolder) holder).end;
              newHolder.buffer = ((NullableVarCharHolder) holder).buffer;

              return newHolder;
            } else {
              throw new UnsupportedOperationException("You can not convert a null value into a non-null value!");
            }
          case REPEATED:
            return holder;
        }
      case VAR16CHAR :

        switch (type.getMode()) {
          case REQUIRED:
            return holder;
          case OPTIONAL:
            if( ((NullableVar16CharHolder) holder).isSet == 1) {
              Var16CharHolder newHolder = new Var16CharHolder();

              newHolder.start = ((NullableVar16CharHolder) holder).start;
              newHolder.end = ((NullableVar16CharHolder) holder).end;
              newHolder.buffer = ((NullableVar16CharHolder) holder).buffer;

              return newHolder;
            } else {
              throw new UnsupportedOperationException("You can not convert a null value into a non-null value!");
            }
          case REPEATED:
            return holder;
        }
      case BIT :

        switch (type.getMode()) {
          case REQUIRED:
            return holder;
          case OPTIONAL:
            if( ((NullableBitHolder) holder).isSet == 1) {
              BitHolder newHolder = new BitHolder();

              newHolder.value = ((NullableBitHolder) holder).value;

              return newHolder;
            } else {
              throw new UnsupportedOperationException("You can not convert a null value into a non-null value!");
            }
          case REPEATED:
            return holder;
        }
      default:
        throw new UnsupportedOperationException(buildErrorMessage("deNullify", type));
    }
  }

  public static ValueHolder nullify(ValueHolder holder) {
    MajorType type = getValueHolderType(holder);

    switch(type.getMinorType()) {
      case TINYINT :
        switch (type.getMode()) {
          case REQUIRED:
            NullableTinyIntHolder newHolder = new NullableTinyIntHolder();
            newHolder.isSet = 1;
            newHolder.value = ((TinyIntHolder) holder).value;
            return newHolder;
          case OPTIONAL:
            return holder;
          case REPEATED:
            throw new UnsupportedOperationException("You can not convert repeated type " + type + " to nullable type!");
        }
      case UINT1 :
        switch (type.getMode()) {
          case REQUIRED:
            NullableUInt1Holder newHolder = new NullableUInt1Holder();
            newHolder.isSet = 1;
            newHolder.value = ((UInt1Holder) holder).value;
            return newHolder;
          case OPTIONAL:
            return holder;
          case REPEATED:
            throw new UnsupportedOperationException("You can not convert repeated type " + type + " to nullable type!");
        }
      case UINT2 :
        switch (type.getMode()) {
          case REQUIRED:
            NullableUInt2Holder newHolder = new NullableUInt2Holder();
            newHolder.isSet = 1;
            newHolder.value = ((UInt2Holder) holder).value;
            return newHolder;
          case OPTIONAL:
            return holder;
          case REPEATED:
            throw new UnsupportedOperationException("You can not convert repeated type " + type + " to nullable type!");
        }
      case SMALLINT :
        switch (type.getMode()) {
          case REQUIRED:
            NullableSmallIntHolder newHolder = new NullableSmallIntHolder();
            newHolder.isSet = 1;
            newHolder.value = ((SmallIntHolder) holder).value;
            return newHolder;
          case OPTIONAL:
            return holder;
          case REPEATED:
            throw new UnsupportedOperationException("You can not convert repeated type " + type + " to nullable type!");
        }
      case INT :
        switch (type.getMode()) {
          case REQUIRED:
            NullableIntHolder newHolder = new NullableIntHolder();
            newHolder.isSet = 1;
            newHolder.value = ((IntHolder) holder).value;
            return newHolder;
          case OPTIONAL:
            return holder;
          case REPEATED:
            throw new UnsupportedOperationException("You can not convert repeated type " + type + " to nullable type!");
        }
      case UINT4 :
        switch (type.getMode()) {
          case REQUIRED:
            NullableUInt4Holder newHolder = new NullableUInt4Holder();
            newHolder.isSet = 1;
            newHolder.value = ((UInt4Holder) holder).value;
            return newHolder;
          case OPTIONAL:
            return holder;
          case REPEATED:
            throw new UnsupportedOperationException("You can not convert repeated type " + type + " to nullable type!");
        }
      case FLOAT4 :
        switch (type.getMode()) {
          case REQUIRED:
            NullableFloat4Holder newHolder = new NullableFloat4Holder();
            newHolder.isSet = 1;
            newHolder.value = ((Float4Holder) holder).value;
            return newHolder;
          case OPTIONAL:
            return holder;
          case REPEATED:
            throw new UnsupportedOperationException("You can not convert repeated type " + type + " to nullable type!");
        }
      case TIME :
        switch (type.getMode()) {
          case REQUIRED:
            NullableTimeHolder newHolder = new NullableTimeHolder();
            newHolder.isSet = 1;
            newHolder.value = ((TimeHolder) holder).value;
            return newHolder;
          case OPTIONAL:
            return holder;
          case REPEATED:
            throw new UnsupportedOperationException("You can not convert repeated type " + type + " to nullable type!");
        }
      case INTERVALYEAR :
        switch (type.getMode()) {
          case REQUIRED:
            NullableIntervalYearHolder newHolder = new NullableIntervalYearHolder();
            newHolder.isSet = 1;
            newHolder.value = ((IntervalYearHolder) holder).value;
            return newHolder;
          case OPTIONAL:
            return holder;
          case REPEATED:
            throw new UnsupportedOperationException("You can not convert repeated type " + type + " to nullable type!");
        }
      case DECIMAL9 :
        switch (type.getMode()) {
          case REQUIRED:
            NullableDecimal9Holder newHolder = new NullableDecimal9Holder();
            newHolder.isSet = 1;
            newHolder.value = ((Decimal9Holder) holder).value;
            newHolder.scale = ((Decimal9Holder) holder).scale;
            newHolder.precision = ((Decimal9Holder) holder).precision;
            return newHolder;
          case OPTIONAL:
            return holder;
          case REPEATED:
            throw new UnsupportedOperationException("You can not convert repeated type " + type + " to nullable type!");
        }
      case BIGINT :
        switch (type.getMode()) {
          case REQUIRED:
            NullableBigIntHolder newHolder = new NullableBigIntHolder();
            newHolder.isSet = 1;
            newHolder.value = ((BigIntHolder) holder).value;
            return newHolder;
          case OPTIONAL:
            return holder;
          case REPEATED:
            throw new UnsupportedOperationException("You can not convert repeated type " + type + " to nullable type!");
        }
      case UINT8 :
        switch (type.getMode()) {
          case REQUIRED:
            NullableUInt8Holder newHolder = new NullableUInt8Holder();
            newHolder.isSet = 1;
            newHolder.value = ((UInt8Holder) holder).value;
            return newHolder;
          case OPTIONAL:
            return holder;
          case REPEATED:
            throw new UnsupportedOperationException("You can not convert repeated type " + type + " to nullable type!");
        }
      case FLOAT8 :
        switch (type.getMode()) {
          case REQUIRED:
            NullableFloat8Holder newHolder = new NullableFloat8Holder();
            newHolder.isSet = 1;
            newHolder.value = ((Float8Holder) holder).value;
            return newHolder;
          case OPTIONAL:
            return holder;
          case REPEATED:
            throw new UnsupportedOperationException("You can not convert repeated type " + type + " to nullable type!");
        }
      case DATE :
        switch (type.getMode()) {
          case REQUIRED:
            NullableDateHolder newHolder = new NullableDateHolder();
            newHolder.isSet = 1;
            newHolder.value = ((DateHolder) holder).value;
            return newHolder;
          case OPTIONAL:
            return holder;
          case REPEATED:
            throw new UnsupportedOperationException("You can not convert repeated type " + type + " to nullable type!");
        }
      case TIMESTAMP :
        switch (type.getMode()) {
          case REQUIRED:
            NullableTimeStampHolder newHolder = new NullableTimeStampHolder();
            newHolder.isSet = 1;
            newHolder.value = ((TimeStampHolder) holder).value;
            return newHolder;
          case OPTIONAL:
            return holder;
          case REPEATED:
            throw new UnsupportedOperationException("You can not convert repeated type " + type + " to nullable type!");
        }
      case DECIMAL18 :
        switch (type.getMode()) {
          case REQUIRED:
            NullableDecimal18Holder newHolder = new NullableDecimal18Holder();
            newHolder.isSet = 1;
            newHolder.value = ((Decimal18Holder) holder).value;
            newHolder.scale = ((Decimal18Holder) holder).scale;
            newHolder.precision = ((Decimal18Holder) holder).precision;
            return newHolder;
          case OPTIONAL:
            return holder;
          case REPEATED:
            throw new UnsupportedOperationException("You can not convert repeated type " + type + " to nullable type!");
        }
      case INTERVALDAY :
        switch (type.getMode()) {
          case REQUIRED:
            NullableIntervalDayHolder newHolder = new NullableIntervalDayHolder();
            newHolder.isSet = 1;
            newHolder.days = ((IntervalDayHolder) holder).days;
            newHolder.milliseconds = ((IntervalDayHolder) holder).milliseconds;
            return newHolder;
          case OPTIONAL:
            return holder;
          case REPEATED:
            throw new UnsupportedOperationException("You can not convert repeated type " + type + " to nullable type!");
        }
      case INTERVAL :
        switch (type.getMode()) {
          case REQUIRED:
            NullableIntervalHolder newHolder = new NullableIntervalHolder();
            newHolder.isSet = 1;
            newHolder.months = ((IntervalHolder) holder).months;
            newHolder.days = ((IntervalHolder) holder).days;
            newHolder.milliseconds = ((IntervalHolder) holder).milliseconds;
            return newHolder;
          case OPTIONAL:
            return holder;
          case REPEATED:
            throw new UnsupportedOperationException("You can not convert repeated type " + type + " to nullable type!");
        }
      case DECIMAL28DENSE :
        switch (type.getMode()) {
          case REQUIRED:
            NullableDecimal28DenseHolder newHolder = new NullableDecimal28DenseHolder();
            newHolder.isSet = 1;
            newHolder.start = ((Decimal28DenseHolder) holder).start;
            newHolder.buffer = ((Decimal28DenseHolder) holder).buffer;
            newHolder.scale = ((Decimal28DenseHolder) holder).scale;
            newHolder.precision = ((Decimal28DenseHolder) holder).precision;
            return newHolder;
          case OPTIONAL:
            return holder;
          case REPEATED:
            throw new UnsupportedOperationException("You can not convert repeated type " + type + " to nullable type!");
        }
      case DECIMAL38DENSE :
        switch (type.getMode()) {
          case REQUIRED:
            NullableDecimal38DenseHolder newHolder = new NullableDecimal38DenseHolder();
            newHolder.isSet = 1;
            newHolder.start = ((Decimal38DenseHolder) holder).start;
            newHolder.buffer = ((Decimal38DenseHolder) holder).buffer;
            newHolder.scale = ((Decimal38DenseHolder) holder).scale;
            newHolder.precision = ((Decimal38DenseHolder) holder).precision;
            return newHolder;
          case OPTIONAL:
            return holder;
          case REPEATED:
            throw new UnsupportedOperationException("You can not convert repeated type " + type + " to nullable type!");
        }
      case DECIMAL38SPARSE :
        switch (type.getMode()) {
          case REQUIRED:
            NullableDecimal38SparseHolder newHolder = new NullableDecimal38SparseHolder();
            newHolder.isSet = 1;
            newHolder.start = ((Decimal38SparseHolder) holder).start;
            newHolder.buffer = ((Decimal38SparseHolder) holder).buffer;
            newHolder.scale = ((Decimal38SparseHolder) holder).scale;
            newHolder.precision = ((Decimal38SparseHolder) holder).precision;
            return newHolder;
          case OPTIONAL:
            return holder;
          case REPEATED:
            throw new UnsupportedOperationException("You can not convert repeated type " + type + " to nullable type!");
        }
      case DECIMAL28SPARSE :
        switch (type.getMode()) {
          case REQUIRED:
            NullableDecimal28SparseHolder newHolder = new NullableDecimal28SparseHolder();
            newHolder.isSet = 1;
            newHolder.start = ((Decimal28SparseHolder) holder).start;
            newHolder.buffer = ((Decimal28SparseHolder) holder).buffer;
            newHolder.scale = ((Decimal28SparseHolder) holder).scale;
            newHolder.precision = ((Decimal28SparseHolder) holder).precision;
            return newHolder;
          case OPTIONAL:
            return holder;
          case REPEATED:
            throw new UnsupportedOperationException("You can not convert repeated type " + type + " to nullable type!");
        }
      case VARBINARY :
        switch (type.getMode()) {
          case REQUIRED:
            NullableVarBinaryHolder newHolder = new NullableVarBinaryHolder();
            newHolder.isSet = 1;
            newHolder.start = ((VarBinaryHolder) holder).start;
            newHolder.end = ((VarBinaryHolder) holder).end;
            newHolder.buffer = ((VarBinaryHolder) holder).buffer;
            return newHolder;
          case OPTIONAL:
            return holder;
          case REPEATED:
            throw new UnsupportedOperationException("You can not convert repeated type " + type + " to nullable type!");
        }
      case VARCHAR :
        switch (type.getMode()) {
          case REQUIRED:
            NullableVarCharHolder newHolder = new NullableVarCharHolder();
            newHolder.isSet = 1;
            newHolder.start = ((VarCharHolder) holder).start;
            newHolder.end = ((VarCharHolder) holder).end;
            newHolder.buffer = ((VarCharHolder) holder).buffer;
            return newHolder;
          case OPTIONAL:
            return holder;
          case REPEATED:
            throw new UnsupportedOperationException("You can not convert repeated type " + type + " to nullable type!");
        }
      case VAR16CHAR :
        switch (type.getMode()) {
          case REQUIRED:
            NullableVar16CharHolder newHolder = new NullableVar16CharHolder();
            newHolder.isSet = 1;
            newHolder.start = ((Var16CharHolder) holder).start;
            newHolder.end = ((Var16CharHolder) holder).end;
            newHolder.buffer = ((Var16CharHolder) holder).buffer;
            return newHolder;
          case OPTIONAL:
            return holder;
          case REPEATED:
            throw new UnsupportedOperationException("You can not convert repeated type " + type + " to nullable type!");
        }
      case BIT :
        switch (type.getMode()) {
          case REQUIRED:
            NullableBitHolder newHolder = new NullableBitHolder();
            newHolder.isSet = 1;
            newHolder.value = ((BitHolder) holder).value;
            return newHolder;
          case OPTIONAL:
            return holder;
          case REPEATED:
            throw new UnsupportedOperationException("You can not convert repeated type " + type + " to nullable type!");
        }
      default:
        throw new UnsupportedOperationException(buildErrorMessage("nullify", type));
    }
  }

  public static MajorType getValueHolderType(ValueHolder holder) {

    if (0 == 1) {
      return null;
    }
      else if (holder instanceof TinyIntHolder) {
        return ((TinyIntHolder) holder).TYPE;
      } else if (holder instanceof NullableTinyIntHolder) {
      return ((NullableTinyIntHolder) holder).TYPE;
      }
      else if (holder instanceof UInt1Holder) {
        return ((UInt1Holder) holder).TYPE;
      } else if (holder instanceof NullableUInt1Holder) {
      return ((NullableUInt1Holder) holder).TYPE;
      }
      else if (holder instanceof UInt2Holder) {
        return ((UInt2Holder) holder).TYPE;
      } else if (holder instanceof NullableUInt2Holder) {
      return ((NullableUInt2Holder) holder).TYPE;
      }
      else if (holder instanceof SmallIntHolder) {
        return ((SmallIntHolder) holder).TYPE;
      } else if (holder instanceof NullableSmallIntHolder) {
      return ((NullableSmallIntHolder) holder).TYPE;
      }
      else if (holder instanceof IntHolder) {
        return ((IntHolder) holder).TYPE;
      } else if (holder instanceof NullableIntHolder) {
      return ((NullableIntHolder) holder).TYPE;
      }
      else if (holder instanceof UInt4Holder) {
        return ((UInt4Holder) holder).TYPE;
      } else if (holder instanceof NullableUInt4Holder) {
      return ((NullableUInt4Holder) holder).TYPE;
      }
      else if (holder instanceof Float4Holder) {
        return ((Float4Holder) holder).TYPE;
      } else if (holder instanceof NullableFloat4Holder) {
      return ((NullableFloat4Holder) holder).TYPE;
      }
      else if (holder instanceof TimeHolder) {
        return ((TimeHolder) holder).TYPE;
      } else if (holder instanceof NullableTimeHolder) {
      return ((NullableTimeHolder) holder).TYPE;
      }
      else if (holder instanceof IntervalYearHolder) {
        return ((IntervalYearHolder) holder).TYPE;
      } else if (holder instanceof NullableIntervalYearHolder) {
      return ((NullableIntervalYearHolder) holder).TYPE;
      }
      else if (holder instanceof Decimal9Holder) {
        return ((Decimal9Holder) holder).TYPE;
      } else if (holder instanceof NullableDecimal9Holder) {
      return ((NullableDecimal9Holder) holder).TYPE;
      }
      else if (holder instanceof BigIntHolder) {
        return ((BigIntHolder) holder).TYPE;
      } else if (holder instanceof NullableBigIntHolder) {
      return ((NullableBigIntHolder) holder).TYPE;
      }
      else if (holder instanceof UInt8Holder) {
        return ((UInt8Holder) holder).TYPE;
      } else if (holder instanceof NullableUInt8Holder) {
      return ((NullableUInt8Holder) holder).TYPE;
      }
      else if (holder instanceof Float8Holder) {
        return ((Float8Holder) holder).TYPE;
      } else if (holder instanceof NullableFloat8Holder) {
      return ((NullableFloat8Holder) holder).TYPE;
      }
      else if (holder instanceof DateHolder) {
        return ((DateHolder) holder).TYPE;
      } else if (holder instanceof NullableDateHolder) {
      return ((NullableDateHolder) holder).TYPE;
      }
      else if (holder instanceof TimeStampHolder) {
        return ((TimeStampHolder) holder).TYPE;
      } else if (holder instanceof NullableTimeStampHolder) {
      return ((NullableTimeStampHolder) holder).TYPE;
      }
      else if (holder instanceof Decimal18Holder) {
        return ((Decimal18Holder) holder).TYPE;
      } else if (holder instanceof NullableDecimal18Holder) {
      return ((NullableDecimal18Holder) holder).TYPE;
      }
      else if (holder instanceof IntervalDayHolder) {
        return ((IntervalDayHolder) holder).TYPE;
      } else if (holder instanceof NullableIntervalDayHolder) {
      return ((NullableIntervalDayHolder) holder).TYPE;
      }
      else if (holder instanceof IntervalHolder) {
        return ((IntervalHolder) holder).TYPE;
      } else if (holder instanceof NullableIntervalHolder) {
      return ((NullableIntervalHolder) holder).TYPE;
      }
      else if (holder instanceof Decimal28DenseHolder) {
        return ((Decimal28DenseHolder) holder).TYPE;
      } else if (holder instanceof NullableDecimal28DenseHolder) {
      return ((NullableDecimal28DenseHolder) holder).TYPE;
      }
      else if (holder instanceof Decimal38DenseHolder) {
        return ((Decimal38DenseHolder) holder).TYPE;
      } else if (holder instanceof NullableDecimal38DenseHolder) {
      return ((NullableDecimal38DenseHolder) holder).TYPE;
      }
      else if (holder instanceof Decimal38SparseHolder) {
        return ((Decimal38SparseHolder) holder).TYPE;
      } else if (holder instanceof NullableDecimal38SparseHolder) {
      return ((NullableDecimal38SparseHolder) holder).TYPE;
      }
      else if (holder instanceof Decimal28SparseHolder) {
        return ((Decimal28SparseHolder) holder).TYPE;
      } else if (holder instanceof NullableDecimal28SparseHolder) {
      return ((NullableDecimal28SparseHolder) holder).TYPE;
      }
      else if (holder instanceof VarBinaryHolder) {
        return ((VarBinaryHolder) holder).TYPE;
      } else if (holder instanceof NullableVarBinaryHolder) {
      return ((NullableVarBinaryHolder) holder).TYPE;
      }
      else if (holder instanceof VarCharHolder) {
        return ((VarCharHolder) holder).TYPE;
      } else if (holder instanceof NullableVarCharHolder) {
      return ((NullableVarCharHolder) holder).TYPE;
      }
      else if (holder instanceof Var16CharHolder) {
        return ((Var16CharHolder) holder).TYPE;
      } else if (holder instanceof NullableVar16CharHolder) {
      return ((NullableVar16CharHolder) holder).TYPE;
      }
      else if (holder instanceof BitHolder) {
        return ((BitHolder) holder).TYPE;
      } else if (holder instanceof NullableBitHolder) {
      return ((NullableBitHolder) holder).TYPE;
      }

    throw new UnsupportedOperationException("ValueHolder is not supported for 'getValueHolderType' method.");

  }

}
