
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
package org.apache.arrow.vector.holders;


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







public final class NullableDecimal38SparseHolder implements ValueHolder{
  
  public static final MajorType TYPE = new MajorType(MinorType.DECIMAL38SPARSE, DataMode.OPTIONAL);

  public MajorType getType() {return TYPE;}
  
    public static final int WIDTH = 24;
    
    public int isSet;
    public int start;
    public ArrowBuf buffer;
    public int scale;
    public int precision;
    
    public static final int maxPrecision = 38;
    public static final int nDecimalDigits = 6;
    
    public static int getInteger(int index, int start, ArrowBuf buffer) {
      int value = buffer.getInt(start + (index * 4));

      if (index == 0) {
          /* the first byte contains sign bit, return value without it */
          value = (value & 0x7FFFFFFF);
      }
      return value;
    }

    public static void setInteger(int index, int value, int start, ArrowBuf buffer) {
        buffer.setInt(start + (index * 4), value);
    }
  
    public static void setSign(boolean sign, int start, ArrowBuf buffer) {
      // Set MSB to 1 if sign is negative
      if (sign == true) {
        int value = getInteger(0, start, buffer);
        setInteger(0, (value | 0x80000000), start, buffer);
      }
    }
  
    public static boolean getSign(int start, ArrowBuf buffer) {
      return ((buffer.getInt(start) & 0x80000000) != 0);
    }
    
    @Deprecated
    public int hashCode(){
      throw new UnsupportedOperationException();
    }

    /*
     * Reason for deprecation is that ValueHolders are potential scalar replacements
     * and hence we don't want any methods to be invoked on them.
     */
    @Deprecated
    public String toString(){
      throw new UnsupportedOperationException();
    }
    
    
    
    
}


