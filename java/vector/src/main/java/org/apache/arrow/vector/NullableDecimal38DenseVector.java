
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
 * NullableDecimal38Dense implements a vector of values which could be null.  Elements in the vector
 * are first checked against a fixed length vector of boolean values.  Then the element is retrieved
 * from the base class (if not null).
 *
 * NB: this class is automatically generated from NullableValueVectors.java and ValueVectorTypes.tdd using FreeMarker.
 */
@SuppressWarnings("unused")
public final class NullableDecimal38DenseVector extends BaseDataValueVector implements FixedWidthVector, NullableVector{
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(NullableDecimal38DenseVector.class);

  private final FieldReader reader = new NullableDecimal38DenseReaderImpl(NullableDecimal38DenseVector.this);

  private final MaterializedField bitsField = MaterializedField.create("$bits$", new MajorType(MinorType.UINT1, DataMode.REQUIRED));
  private final UInt1Vector bits = new UInt1Vector(bitsField, allocator);
  private final Decimal38DenseVector values = new Decimal38DenseVector(field, allocator);

  private final Mutator mutator = new Mutator();
  private final Accessor accessor = new Accessor();

  public NullableDecimal38DenseVector(MaterializedField field, BufferAllocator allocator) {
    super(field, allocator);
  }

  @Override
  public FieldReader getReader(){
    return reader;
  }

  @Override
  public int getValueCapacity(){
    return Math.min(bits.getValueCapacity(), values.getValueCapacity());
  }

  @Override
  public ArrowBuf[] getBuffers(boolean clear) {
    final ArrowBuf[] buffers = ObjectArrays.concat(bits.getBuffers(false), values.getBuffers(false), ArrowBuf.class);
    if (clear) {
      for (final ArrowBuf buffer:buffers) {
        buffer.retain(1);
      }
      clear();
    }
    return buffers;
  }

  @Override
  public void close() {
    bits.close();
    values.close();
    super.close();
  }

  @Override
  public void clear() {
    bits.clear();
    values.clear();
    super.clear();
  }

  @Override
  public int getBufferSize(){
    return values.getBufferSize() + bits.getBufferSize();
  }

  @Override
  public int getBufferSizeFor(final int valueCount) {
    if (valueCount == 0) {
      return 0;
    }

    return values.getBufferSizeFor(valueCount)
        + bits.getBufferSizeFor(valueCount);
  }

  @Override
  public ArrowBuf getBuffer() {
    return values.getBuffer();
  }

  @Override
  public Decimal38DenseVector getValuesVector() {
    return values;
  }

  @Override
  public void setInitialCapacity(int numRecords) {
    bits.setInitialCapacity(numRecords);
    values.setInitialCapacity(numRecords);
  }

//  @Override
//  public SerializedField.Builder getMetadataBuilder() {
//    return super.getMetadataBuilder()
//      .addChild(bits.getMetadata())
//      .addChild(values.getMetadata());
//  }

  @Override
  public void allocateNew() {
    if(!allocateNewSafe()){
      throw new OutOfMemoryException("Failure while allocating buffer.");
    }
  }

  @Override
  public boolean allocateNewSafe() {
    /* Boolean to keep track if all the memory allocations were successful
     * Used in the case of composite vectors when we need to allocate multiple
     * buffers for multiple vectors. If one of the allocations failed we need to
     * clear all the memory that we allocated
     */
    boolean success = false;
    try {
      success = values.allocateNewSafe() && bits.allocateNewSafe();
    } finally {
      if (!success) {
        clear();
      }
    }
    bits.zeroVector();
    mutator.reset();
    accessor.reset();
    return success;
  }

  @Override
  public void allocateNew(int valueCount) {
    try {
      values.allocateNew(valueCount);
      bits.allocateNew(valueCount+1);
    } catch(OutOfMemoryException e) {
      clear();
      throw e;
    }
    bits.zeroVector();
    mutator.reset();
    accessor.reset();
  }

  @Override
  public void reset() {
    bits.zeroVector();
    mutator.reset();
    accessor.reset();
    super.reset();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void zeroVector() {
    bits.zeroVector();
    values.zeroVector();
  }


//  @Override
//  public void load(SerializedField metadata, ArrowBuf buffer) {
//    clear();
    // the bits vector is the first child (the order in which the children are added in getMetadataBuilder is significant)
//    final SerializedField bitsField = metadata.getChild(0);
//    bits.load(bitsField, buffer);
//
//    final int capacity = buffer.capacity();
//    final int bitsLength = bitsField.getBufferLength();
//    final SerializedField valuesField = metadata.getChild(1);
//    values.load(valuesField, buffer.slice(bitsLength, capacity - bitsLength));
//  }

  @Override
  public TransferPair getTransferPair(BufferAllocator allocator){
    return new TransferImpl(getField(), allocator);
  }

  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator){
    return new TransferImpl(getField().withPath(ref), allocator);
  }

  @Override
  public TransferPair makeTransferPair(ValueVector to) {
    return new TransferImpl((NullableDecimal38DenseVector) to);
  }

  public void transferTo(NullableDecimal38DenseVector target){
    bits.transferTo(target.bits);
    values.transferTo(target.values);
    clear();
  }

  public void splitAndTransferTo(int startIndex, int length, NullableDecimal38DenseVector target) {
    bits.splitAndTransferTo(startIndex, length, target.bits);
    values.splitAndTransferTo(startIndex, length, target.values);
  }

  private class TransferImpl implements TransferPair {
    NullableDecimal38DenseVector to;

    public TransferImpl(MaterializedField field, BufferAllocator allocator){
      to = new NullableDecimal38DenseVector(field, allocator);
    }

    public TransferImpl(NullableDecimal38DenseVector to){
      this.to = to;
    }

    @Override
    public NullableDecimal38DenseVector getTo(){
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
      to.copyFromSafe(fromIndex, toIndex, NullableDecimal38DenseVector.this);
    }
  }

  @Override
  public Accessor getAccessor(){
    return accessor;
  }

  @Override
  public Mutator getMutator(){
    return mutator;
  }

  public Decimal38DenseVector convertToRequiredVector(){
    Decimal38DenseVector v = new Decimal38DenseVector(getField().getOtherNullableVersion(), allocator);
    if (v.data != null) {
      v.data.release(1);
    }
    v.data = values.data;
    v.data.retain(1);
    clear();
    return v;
  }

  public void copyFrom(int fromIndex, int thisIndex, NullableDecimal38DenseVector from){
    final Accessor fromAccessor = from.getAccessor();
    if (!fromAccessor.isNull(fromIndex)) {
      mutator.set(thisIndex, fromAccessor.get(fromIndex));
    }
  }

  public void copyFromSafe(int fromIndex, int thisIndex, Decimal38DenseVector from){
    values.copyFromSafe(fromIndex, thisIndex, from);
    bits.getMutator().setSafe(thisIndex, 1);
  }

  public void copyFromSafe(int fromIndex, int thisIndex, NullableDecimal38DenseVector from){
    bits.copyFromSafe(fromIndex, thisIndex, from.bits);
    values.copyFromSafe(fromIndex, thisIndex, from.values);
  }

  public final class Accessor extends BaseDataValueVector.BaseAccessor  {
    final UInt1Vector.Accessor bAccessor = bits.getAccessor();
    final Decimal38DenseVector.Accessor vAccessor = values.getAccessor();

    /**
     * Get the element at the specified position.
     *
     * @param   index   position of the value
     * @return  value of the element, if not null
     * @throws  NullValueException if the value is null
     */
    public ArrowBuf get(int index) {
      if (isNull(index)) {
          throw new IllegalStateException("Can't get a null value");
      }
      return vAccessor.get(index);
    }

    @Override
    public boolean isNull(int index) {
      return isSet(index) == 0;
    }

    public int isSet(int index){
      return bAccessor.get(index);
    }


    public void get(int index, NullableDecimal38DenseHolder holder){
      vAccessor.get(index, holder);
      holder.isSet = bAccessor.get(index);

      holder.scale = getField().getScale();
      holder.precision = getField().getPrecision();
    }

    @Override
    public BigDecimal getObject(int index) {
      if (isNull(index)) {
          return null;
      }else{
        return vAccessor.getObject(index);
      }
    }


    @Override
    public int getValueCount(){
      return bits.getAccessor().getValueCount();
    }

    public void reset(){}
  }

  public final class Mutator extends BaseDataValueVector.BaseMutator implements NullableVectorDefinitionSetter {
    private int setCount;
    

    private Mutator(){
    }

    public Decimal38DenseVector getVectorWithValues(){
      return values;
    }

    @Override
    public void setIndexDefined(int index){
      bits.getMutator().set(index, 1);
    }

    /**
     * Set the variable length element at the specified index to the supplied byte array.
     *
     * @param index   position of the bit to set
     * @param bytes   array of bytes to write
     */
    public void set(int index, ArrowBuf value) {
      setCount++;
      final Decimal38DenseVector.Mutator valuesMutator = values.getMutator();
      final UInt1Vector.Mutator bitsMutator = bits.getMutator();
      bitsMutator.set(index, 1);
      valuesMutator.set(index, value);
      
    }


    public void setSafe(int index, byte[] value, int start, int length) {
      throw new UnsupportedOperationException();
    }

    public void setSafe(int index, ByteBuffer value, int start, int length) {
      throw new UnsupportedOperationException();
    }

    public void setNull(int index){
      bits.getMutator().setSafe(index, 0);
    }

    public void setSkipNull(int index, Decimal38DenseHolder holder){
      values.getMutator().set(index, holder);
    }

    public void setSkipNull(int index, NullableDecimal38DenseHolder holder){
      values.getMutator().set(index, holder);
    }


    public void set(int index, NullableDecimal38DenseHolder holder){
      final Decimal38DenseVector.Mutator valuesMutator = values.getMutator();
      bits.getMutator().set(index, holder.isSet);
      valuesMutator.set(index, holder);
      
    }

    public void set(int index, Decimal38DenseHolder holder){
      final Decimal38DenseVector.Mutator valuesMutator = values.getMutator();
      bits.getMutator().set(index, 1);
      valuesMutator.set(index, holder);
      
    }

    public boolean isSafe(int outIndex) {
      return outIndex < NullableDecimal38DenseVector.this.getValueCapacity();
    }

    public void set(int index, int isSet, int startField, ArrowBuf bufferField ){
      final Decimal38DenseVector.Mutator valuesMutator = values.getMutator();
      bits.getMutator().set(index, isSet);
      valuesMutator.set(index, startField, bufferField);
      
    }

    public void setSafe(int index, int isSet, int startField, ArrowBuf bufferField ) {

      bits.getMutator().setSafe(index, isSet);
      values.getMutator().setSafe(index, startField, bufferField);
      setCount++;
      
    }


    public void setSafe(int index, NullableDecimal38DenseHolder value) {

      bits.getMutator().setSafe(index, value.isSet);
      values.getMutator().setSafe(index, value);
      setCount++;
      
    }

    public void setSafe(int index, Decimal38DenseHolder value) {

      bits.getMutator().setSafe(index, 1);
      values.getMutator().setSafe(index, value);
      setCount++;
      
    }


    @Override
    public void setValueCount(int valueCount) {
      assert valueCount >= 0;
      values.getMutator().setValueCount(valueCount);
      bits.getMutator().setValueCount(valueCount);
    }

    @Override
    public void generateTestData(int valueCount){
      bits.getMutator().generateTestDataAlt(valueCount);
      values.getMutator().generateTestData(valueCount);
      
      setValueCount(valueCount);
    }

    @Override
    public void reset(){
      setCount = 0;
      
    }
  }
}


