

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
package org.apache.arrow.vector.complex.reader;


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
public interface BaseReader extends Positionable{
  MajorType getType();
  MaterializedField getField();
  void reset();
  void read(UnionHolder holder);
  void read(int index, UnionHolder holder);
  void copyAsValue(UnionWriter writer);
  boolean isSet();

  public interface MapReader extends BaseReader, Iterable<String>{
    FieldReader reader(String name);
  }
  
  public interface RepeatedMapReader extends MapReader{
    boolean next();
    int size();
    void copyAsValue(MapWriter writer);
  }
  
  public interface ListReader extends BaseReader{
    FieldReader reader(); 
  }
  
  public interface RepeatedListReader extends ListReader{
    boolean next();
    int size();
    void copyAsValue(ListWriter writer);
  }
  
  public interface ScalarReader extends  
   TinyIntReader,  UInt1Reader,  UInt2Reader,  SmallIntReader,  IntReader,  UInt4Reader,  Float4Reader,  TimeReader,  IntervalYearReader,  Decimal9Reader,  BigIntReader,  UInt8Reader,  Float8Reader,  DateReader,  TimeStampReader,  Decimal18Reader,  IntervalDayReader,  IntervalReader,  Decimal28DenseReader,  Decimal38DenseReader,  Decimal38SparseReader,  Decimal28SparseReader,  VarBinaryReader,  VarCharReader,  Var16CharReader,  BitReader,  
   RepeatedTinyIntReader,  RepeatedUInt1Reader,  RepeatedUInt2Reader,  RepeatedSmallIntReader,  RepeatedIntReader,  RepeatedUInt4Reader,  RepeatedFloat4Reader,  RepeatedTimeReader,  RepeatedIntervalYearReader,  RepeatedDecimal9Reader,  RepeatedBigIntReader,  RepeatedUInt8Reader,  RepeatedFloat8Reader,  RepeatedDateReader,  RepeatedTimeStampReader,  RepeatedDecimal18Reader,  RepeatedIntervalDayReader,  RepeatedIntervalReader,  RepeatedDecimal28DenseReader,  RepeatedDecimal38DenseReader,  RepeatedDecimal38SparseReader,  RepeatedDecimal28SparseReader,  RepeatedVarBinaryReader,  RepeatedVarCharReader,  RepeatedVar16CharReader,  RepeatedBitReader, 
  BaseReader {}
  
  interface ComplexReader{
    MapReader rootAsMap();
    ListReader rootAsList();
    boolean rootIsMap();
    boolean ok();
  }
}

