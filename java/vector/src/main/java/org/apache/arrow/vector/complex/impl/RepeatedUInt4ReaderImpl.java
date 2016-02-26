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
public class RepeatedUInt4ReaderImpl extends AbstractFieldReader {
  
  private final RepeatedUInt4Vector vector;
  
  public RepeatedUInt4ReaderImpl(RepeatedUInt4Vector vector){
    super();
    this.vector = vector;
  }

  public MajorType getType(){
    return vector.getField().getType();
  }

  public MaterializedField getField(){
    return vector.getField();
  }
  
  public boolean isSet(){
    return true;
  }


  
  

  public void copyAsValue(UInt4Writer writer){
    RepeatedUInt4WriterImpl impl = (RepeatedUInt4WriterImpl) writer;
    impl.vector.copyFromSafe(idx(), impl.idx(), vector);
  }
  
  public void copyAsField(String name, MapWriter writer){
    RepeatedUInt4WriterImpl impl = (RepeatedUInt4WriterImpl)  writer.list(name).uInt4();
    impl.vector.copyFromSafe(idx(), impl.idx(), vector);
  }
  
  public int size(){
    return vector.getAccessor().getInnerValueCountAt(idx());
  }
  
  public void read(int arrayIndex, UInt4Holder h){
    vector.getAccessor().get(idx(), arrayIndex, h);
  }
  public void read(int arrayIndex, NullableUInt4Holder h){
    vector.getAccessor().get(idx(), arrayIndex, h);
  }
  
  public Integer readInteger(int arrayIndex){
    return vector.getAccessor().getSingleObject(idx(), arrayIndex);
  }

  
  public List<Object> readObject(){
    return (List<Object>) (Object) vector.getAccessor().getObject(idx());
  }
  
}
