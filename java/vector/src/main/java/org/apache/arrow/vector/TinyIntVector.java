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
 * TinyInt implements a vector of fixed width values.  Elements in the vector are accessed
 * by position, starting from the logical start of the vector.  Values should be pushed onto the
 * vector sequentially, but may be randomly accessed.
 *   The width of each element is 1 byte(s)
 *   The equivalent Java primitive is 'byte'
 *
 * NB: this class is automatically generated from FixedValueVectors.java and ValueVectorTypes.tdd using FreeMarker.
 */
public final class TinyIntVector extends BaseDataValueVector implements FixedWidthVector{
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TinyIntVector.class);

  private final FieldReader reader = new TinyIntReaderImpl(TinyIntVector.this);
  private final Accessor accessor = new Accessor();
  private final Mutator mutator = new Mutator();

  private int allocationSizeInBytes = INITIAL_VALUE_ALLOCATION * 1;
  private int allocationMonitor = 0;

  public TinyIntVector(MaterializedField field, BufferAllocator allocator) {
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
    return valueCount * 1;
  }

  @Override
  public int getValueCapacity(){
    return (int) (data.capacity() *1.0 / 1);
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
    final long size = 1L * valueCount * 1;
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
    allocateBytes(valueCount * 1);
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
//    final int expectedLength = valueCount * 1;
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
    return new TransferImpl((TinyIntVector) to);
  }

  public void transferTo(TinyIntVector target){
    target.clear();
    target.data = data.transferOwnership(target.allocator).buffer;
    target.data.writerIndex(data.writerIndex());
    clear();
  }

  public void splitAndTransferTo(int startIndex, int length, TinyIntVector target) {
    final int startPoint = startIndex * 1;
    final int sliceLength = length * 1;
    target.clear();
    target.data = data.slice(startPoint, sliceLength).transferOwnership(target.allocator).buffer;
    target.data.writerIndex(sliceLength);
  }

  private class TransferImpl implements TransferPair{
    private TinyIntVector to;

    public TransferImpl(MaterializedField field, BufferAllocator allocator){
      to = new TinyIntVector(field, allocator);
    }

    public TransferImpl(TinyIntVector to) {
      this.to = to;
    }

    @Override
    public TinyIntVector getTo(){
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
      to.copyFromSafe(fromIndex, toIndex, TinyIntVector.this);
    }
  }

  public void copyFrom(int fromIndex, int thisIndex, TinyIntVector from){
 
    data.setByte(thisIndex * 1,
        from.data.getByte(fromIndex * 1)
    );
     
  }

  public void copyFromSafe(int fromIndex, int thisIndex, TinyIntVector from){
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
      return data.writerIndex() / 1;
    }

    @Override
    public boolean isNull(int index){
      return false;
    }

 

    public byte get(int index) {
      return data.getByte(index * 1);
    }


    @Override
    public Byte getObject(int index) {
      return get(index);
    }
    public byte getPrimitiveObject(int index) {
      return get(index);
    }

    public void get(int index, TinyIntHolder holder){

      holder.value = data.getByte(index * 1);
    }

    public void get(int index, NullableTinyIntHolder holder){
      holder.isSet = 1;
      holder.value = data.getByte(index * 1);
    }


    
 }

 /**
  * TinyInt.Mutator implements a mutable vector of fixed width values.  Elements in the
  * vector are accessed by position from the logical start of the vector.  Values should be pushed
  * onto the vector sequentially, but may be randomly accessed.
  *   The width of each element is 1 byte(s)
  *   The equivalent Java primitive is 'byte'
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
 
   public void set(int index, int value) {
     data.setByte(index * 1, value);
   }

   public void setSafe(int index, int value) {
     while(index >= getValueCapacity()) {
       reAlloc();
     }
     set(index, value);
   }

   protected void set(int index, TinyIntHolder holder){
     data.setByte(index * 1, holder.value);
   }

   public void setSafe(int index, TinyIntHolder holder){
     while(index >= getValueCapacity()) {
       reAlloc();
     }
     set(index, holder);
   }

   protected void set(int index, NullableTinyIntHolder holder){
     data.setByte(index * 1, holder.value);
   }

   public void setSafe(int index, NullableTinyIntHolder holder){
     while(index >= getValueCapacity()) {
       reAlloc();
     }
     set(index, holder);
   }

   @Override
   public void generateTestData(int size) {
     setValueCount(size);
     boolean even = true;
     final int valueCount = getAccessor().getValueCount();
     for(int i = 0; i < valueCount; i++, even = !even) {
       if(even){
         set(i, Byte.MIN_VALUE);
       }else{
         set(i, Byte.MAX_VALUE);
       }
     }
   }

   public void generateTestDataAlt(int size) {
     setValueCount(size);
     boolean even = true;
     final int valueCount = getAccessor().getValueCount();
     for(int i = 0; i < valueCount; i++, even = !even) {
       if(even){
         set(i, (byte) 1);
       }else{
         set(i, (byte) 0);
       }
     }
   }

   

   @Override
   public void setValueCount(int valueCount) {
     final int currentValueCapacity = getValueCapacity();
     final int idx = (1 * valueCount);
     while(valueCount > getValueCapacity()) {
       reAlloc();
     }
     if (valueCount > 0 && currentValueCapacity > valueCount * 2) {
       incrementAllocationMonitor();
     } else if (allocationMonitor > 0) {
       allocationMonitor = 0;
     }
     VectorTrimmer.trim(data, idx);
     data.writerIndex(valueCount * 1);
   }
 }
}

 

