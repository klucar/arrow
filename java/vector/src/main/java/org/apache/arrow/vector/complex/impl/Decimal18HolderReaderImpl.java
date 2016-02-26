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







import java.math.BigDecimal;
import java.math.BigInteger;

import org.joda.time.Period;

// Source code generated using FreeMarker template HolderReaderImpl.java

@SuppressWarnings("unused")
public class Decimal18HolderReaderImpl extends AbstractFieldReader {

  private Decimal18Holder holder;

  public Decimal18HolderReaderImpl(Decimal18Holder holder) {
    this.holder = holder;
  }

  @Override
  public int size() {
    throw new UnsupportedOperationException("You can't call size on a Holder value reader.");
  }

  @Override
  public boolean next() {
    throw new UnsupportedOperationException("You can't call next on a single value reader.");

  }

  @Override
  public void setPosition(int index) {
    throw new UnsupportedOperationException("You can't call next on a single value reader.");
  }

  @Override
  public MajorType getType() {
    return this.holder.TYPE;
  }

  @Override
  public boolean isSet() {
    return true;
    
  }

@Override
  public void read(Decimal18Holder h) {
    h.value = holder.value;
    h.scale = holder.scale;
    h.precision = holder.precision;
  }

  @Override
  public void read(NullableDecimal18Holder h) {
    h.value = holder.value;
    h.scale = holder.scale;
    h.precision = holder.precision;
    h.isSet = isSet() ? 1 : 0;
  }


  @Override
  public BigDecimal readBigDecimal(){

      BigInteger value = BigInteger.valueOf(holder.value);
      return new BigDecimal(value, holder.scale);


  }

  @Override
  public Object readObject() {
    return readSingleObject();
  }

  private Object readSingleObject() {

      BigInteger value = BigInteger.valueOf(holder.value);
      return new BigDecimal(value, holder.scale);

  }

  public void copyAsValue(Decimal18Writer writer){
    writer.write(holder);
  }
}



