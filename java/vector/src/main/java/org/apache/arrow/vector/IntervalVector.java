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
package org.apache.arrow.vector;


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







/**
 * Interval implements a vector of fixed width values.  Elements in the vector are accessed
 * by position, starting from the logical start of the vector.  Values should be pushed onto the
 * vector sequentially, but may be randomly accessed.
 *   The width of each element is 16 byte(s)
 *   The equivalent Java primitive is 'ArrowBuf'
 *
 * NB: this class is automatically generated from FixedValueVectors.java and ValueVectorTypes.tdd using FreeMarker.
 */
public final class IntervalVector extends BaseDataValueVector implements FixedWidthVector{
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(IntervalVector.class);

  private final FieldReader reader = new IntervalReaderImpl(IntervalVector.this);
  private final Accessor accessor = new Accessor();
  private final Mutator mutator = new Mutator();

  private int allocationSizeInBytes = INITIAL_VALUE_ALLOCATION * 16;
  private int allocationMonitor = 0;

  public IntervalVector(MaterializedField field, BufferAllocator allocator) {
    super(field, allocator);
  }

  @Override
  public FieldReader getReader(){
    return reader;
  }

  @Override
  public int getBufferSizeFor(final int valueCount) {
    if (valueCount == 0) {
      return 0;
    }
    return valueCount * 16;
  }

  @Override
  public int getValueCapacity(){
    return (int) (data.capacity() *1.0 / 16);
  }

  @Override
  public Accessor getAccessor(){
    return accessor;
  }

  @Override
  public Mutator getMutator(){
    return mutator;
  }

  @Override
  public void setInitialCapacity(final int valueCount) {
    final long size = 1L * valueCount * 16;
    if (size > MAX_ALLOCATION_SIZE) {
      throw new OversizedAllocationException("Requested amount of memory is more than max allowed allocation size");
    }
    allocationSizeInBytes = (int)size;
  }

  @Override
  public void allocateNew() {
    if(!allocateNewSafe()){
      throw new OutOfMemoryException("Failure while allocating buffer.");
    }
  }

  @Override
  public boolean allocateNewSafe() {
    long curAllocationSize = allocationSizeInBytes;
    if (allocationMonitor > 10) {
      curAllocationSize = Math.max(8, curAllocationSize / 2);
      allocationMonitor = 0;
    } else if (allocationMonitor < -2) {
      curAllocationSize = allocationSizeInBytes * 2L;
      allocationMonitor = 0;
    }

    try{
      allocateBytes(curAllocationSize);
    } catch (RuntimeException ex) {
      return false;
    }
    return true;
  }

  /**
   * Allocate a new buffer that supports setting at least the provided number of values. May actually be sized bigger
   * depending on underlying buffer rounding size. Must be called prior to using the ValueVector.
   *
   * Note that the maximum number of values a vector can allocate is Integer.MAX_VALUE / value width.
   *
   * @param valueCount
   * @throws org.apache.arrow.memory.OutOfMemoryException if it can't allocate the new buffer
   */
  @Override
  public void allocateNew(final int valueCount) {
    allocateBytes(valueCount * 16);
  }

  @Override
  public void reset() {
    allocationSizeInBytes = INITIAL_VALUE_ALLOCATION;
    allocationMonitor = 0;
    zeroVector();
    super.reset();
    }

  private void allocateBytes(final long size) {
    if (size > MAX_ALLOCATION_SIZE) {
      throw new OversizedAllocationException("Requested amount of memory is more than max allowed allocation size");
    }

    final int curSize = (int)size;
    clear();
    data = allocator.buffer(curSize);
    data.readerIndex(0);
    allocationSizeInBytes = curSize;
  }

/**
 * Allocate new buffer with double capacity, and copy data into the new buffer. Replace vector's buffer with new buffer, and release old one
 *
 * @throws org.apache.arrow.memory.OutOfMemoryException if it can't allocate the new buffer
 */
  public void reAlloc() {
    final long newAllocationSize = allocationSizeInBytes * 2L;
    if (newAllocationSize > MAX_ALLOCATION_SIZE)  {
      throw new OversizedAllocationException("Unable to expand the buffer. Max allowed buffer size is reached.");
    }

    logger.debug("Reallocating vector [{}]. # of bytes: [{}] -> [{}]", field, allocationSizeInBytes, newAllocationSize);
    final ArrowBuf newBuf = allocator.buffer((int)newAllocationSize);
    newBuf.setBytes(0, data, 0, data.capacity());
    final int halfNewCapacity = newBuf.capacity() / 2;
    newBuf.setZero(halfNewCapacity, halfNewCapacity);
    newBuf.writerIndex(data.writerIndex());
    data.release(1);
    data = newBuf;
    allocationSizeInBytes = (int)newAllocationSize;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void zeroVector() {
    data.setZero(0, data.capacity());
  }

//  @Override
//  public void load(SerializedField metadata, ArrowBuf buffer) {
//    Preconditions.checkArgument(this.field.getPath().equals(metadata.getNamePart().getName()), "The field %s doesn't match the provided metadata %s.", this.field, metadata);
//    final int actualLength = metadata.getBufferLength();
//    final int valueCount = metadata.getValueCount();
//    final int expectedLength = valueCount * 16;
//    assert actualLength == expectedLength : String.format("Expected to load %d bytes but actually loaded %d bytes", expectedLength, actualLength);
//
//    clear();
//    if (data != null) {
//      data.release(1);
//    }
//    data = buffer.slice(0, actualLength);
//    data.retain(1);
//    data.writerIndex(actualLength);
//    }

  public TransferPair getTransferPair(BufferAllocator allocator){
    return new TransferImpl(getField(), allocator);
  }

  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator){
    return new TransferImpl(getField().withPath(ref), allocator);
  }

  @Override
  public TransferPair makeTransferPair(ValueVector to) {
    return new TransferImpl((IntervalVector) to);
  }

  public void transferTo(IntervalVector target){
    target.clear();
    target.data = data.transferOwnership(target.allocator).buffer;
    target.data.writerIndex(data.writerIndex());
    clear();
  }

  public void splitAndTransferTo(int startIndex, int length, IntervalVector target) {
    final int startPoint = startIndex * 16;
    final int sliceLength = length * 16;
    target.clear();
    target.data = data.slice(startPoint, sliceLength).transferOwnership(target.allocator).buffer;
    target.data.writerIndex(sliceLength);
  }

  private class TransferImpl implements TransferPair{
    private IntervalVector to;

    public TransferImpl(MaterializedField field, BufferAllocator allocator){
      to = new IntervalVector(field, allocator);
    }

    public TransferImpl(IntervalVector to) {
      this.to = to;
    }

    @Override
    public IntervalVector getTo(){
      return to;
    }

    @Override
    public void transfer(){
      transferTo(to);
    }

    @Override
    public void splitAndTransfer(int startIndex, int length) {
      splitAndTransferTo(startIndex, length, to);
    }

    @Override
    public void copyValueSafe(int fromIndex, int toIndex) {
      to.copyFromSafe(fromIndex, toIndex, IntervalVector.this);
    }
  }

  public void copyFrom(int fromIndex, int thisIndex, IntervalVector from){
    from.data.getBytes(fromIndex * 16, data, thisIndex * 16, 16);
     
  }

  public void copyFromSafe(int fromIndex, int thisIndex, IntervalVector from){
    while(thisIndex >= getValueCapacity()) {
        reAlloc();
    }
    copyFrom(fromIndex, thisIndex, from);
  }

  public void decrementAllocationMonitor() {
    if (allocationMonitor > 0) {
      allocationMonitor = 0;
    }
    --allocationMonitor;
  }

  private void incrementAllocationMonitor() {
    ++allocationMonitor;
  }

  public final class Accessor extends BaseDataValueVector.BaseAccessor {
    @Override
    public int getValueCount() {
      return data.writerIndex() / 16;
    }

    @Override
    public boolean isNull(int index){
      return false;
    }


    public ArrowBuf get(int index) {
      return data.slice(index * 16, 16);
    }

    public void get(int index, IntervalHolder holder){

      final int offsetIndex = index * 16;
      holder.months = data.getInt(offsetIndex);
      holder.days = data.getInt(offsetIndex + 4);
      holder.milliseconds = data.getInt(offsetIndex + 8);
    }

    public void get(int index, NullableIntervalHolder holder){
      final int offsetIndex = index * 16;
      holder.isSet = 1;
      holder.months = data.getInt(offsetIndex);
      holder.days = data.getInt(offsetIndex + 4);
      holder.milliseconds = data.getInt(offsetIndex + 8);
    }

    @Override
    public Period getObject(int index) {
      final int offsetIndex = index * 16;
      final int months  = data.getInt(offsetIndex);
      final int days    = data.getInt(offsetIndex + 4);
      final int millis = data.getInt(offsetIndex + 8);
      final Period p = new Period();
      return p.plusMonths(months).plusDays(days).plusMillis(millis);
    }

    public StringBuilder getAsStringBuilder(int index) {

      final int offsetIndex = index * 16;

      int months  = data.getInt(offsetIndex);
      final int days    = data.getInt(offsetIndex + 4);
      int millis = data.getInt(offsetIndex + 8);

      final int years  = (months / org.apache.arrow.vector.util.DateUtility.yearsToMonths);
      months = (months % org.apache.arrow.vector.util.DateUtility.yearsToMonths);

      final int hours  = millis / (org.apache.arrow.vector.util.DateUtility.hoursToMillis);
      millis     = millis % (org.apache.arrow.vector.util.DateUtility.hoursToMillis);

      final int minutes = millis / (org.apache.arrow.vector.util.DateUtility.minutesToMillis);
      millis      = millis % (org.apache.arrow.vector.util.DateUtility.minutesToMillis);

      final long seconds = millis / (org.apache.arrow.vector.util.DateUtility.secondsToMillis);
      millis      = millis % (org.apache.arrow.vector.util.DateUtility.secondsToMillis);

      final String yearString = (Math.abs(years) == 1) ? " year " : " years ";
      final String monthString = (Math.abs(months) == 1) ? " month " : " months ";
      final String dayString = (Math.abs(days) == 1) ? " day " : " days ";


      return(new StringBuilder().
             append(years).append(yearString).
             append(months).append(monthString).
             append(days).append(dayString).
             append(hours).append(":").
             append(minutes).append(":").
             append(seconds).append(".").
             append(millis));
    }

     
 }

 /**
  * Interval.Mutator implements a mutable vector of fixed width values.  Elements in the
  * vector are accessed by position from the logical start of the vector.  Values should be pushed
  * onto the vector sequentially, but may be randomly accessed.
  *   The width of each element is 16 byte(s)
  *   The equivalent Java primitive is 'ArrowBuf'
  *
  * NB: this class is automatically generated from ValueVectorTypes.tdd using FreeMarker.
  */
  public final class Mutator extends BaseDataValueVector.BaseMutator {

    private Mutator(){};
   /**
    * Set the element at the given index to the given value.  Note that widths smaller than
    * 32 bits are handled by the ArrowBuf interface.
    *
    * @param index   position of the bit to set
    * @param value   value to set
    */
   public void set(int index, ArrowBuf value) {
     data.setBytes(index * 16, value, 0, 16);
   }

   public void setSafe(int index, ArrowBuf value) {
     while(index >= getValueCapacity()) {
       reAlloc();
     }
     data.setBytes(index * 16, value, 0, 16);
   }

   public void set(int index, int months, int days, int milliseconds){
     final int offsetIndex = index * 16;
     data.setInt(offsetIndex, months);
     data.setInt((offsetIndex + 4), days);
     data.setInt((offsetIndex + 8), milliseconds);
   }

   protected void set(int index, IntervalHolder holder){
     set(index, holder.months, holder.days, holder.milliseconds);
   }

   protected void set(int index, NullableIntervalHolder holder){
     set(index, holder.months, holder.days, holder.milliseconds);
   }

   public void setSafe(int index, int months, int days, int milliseconds){
     while(index >= getValueCapacity()) {
       reAlloc();
     }
     set(index, months, days, milliseconds);
   }

   public void setSafe(int index, NullableIntervalHolder holder){
     setSafe(index, holder.months, holder.days, holder.milliseconds);
   }

   public void setSafe(int index, IntervalHolder holder){
     setSafe(index, holder.months, holder.days, holder.milliseconds);
   }


   @Override
   public void generateTestData(int count) {
     setValueCount(count);
     boolean even = true;
     final int valueCount = getAccessor().getValueCount();
     for(int i = 0; i < valueCount; i++, even = !even) {
       final byte b = even ? Byte.MIN_VALUE : Byte.MAX_VALUE;
       for(int w = 0; w < 16; w++){
         data.setByte(i + w, b);
       }
     }
   }

    

   @Override
   public void setValueCount(int valueCount) {
     final int currentValueCapacity = getValueCapacity();
     final int idx = (16 * valueCount);
     while(valueCount > getValueCapacity()) {
       reAlloc();
     }
     if (valueCount > 0 && currentValueCapacity > valueCount * 2) {
       incrementAllocationMonitor();
     } else if (allocationMonitor > 0) {
       allocationMonitor = 0;
     }
     VectorTrimmer.trim(data, idx);
     data.writerIndex(valueCount * 16);
   }
 }
}

 

