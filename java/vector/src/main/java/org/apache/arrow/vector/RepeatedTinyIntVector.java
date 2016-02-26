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
 * RepeatedTinyInt implements a vector with multple values per row (e.g. JSON array or
 * repeated protobuf field).  The implementation uses two additional value vectors; one to convert
 * the index offset to the underlying element offset, and another to store the number of values
 * in the vector.
 *
 * NB: this class is automatically generated from RepeatedValueVectors.java and ValueVectorTypes.tdd using FreeMarker.
 */

public final class RepeatedTinyIntVector extends BaseRepeatedValueVector implements RepeatedFixedWidthVectorLike {
  //private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RepeatedTinyIntVector.class);

  // we maintain local reference to concrete vector type for performance reasons.
  private TinyIntVector values;
  private final FieldReader reader = new RepeatedTinyIntReaderImpl(RepeatedTinyIntVector.this);
  private final Mutator mutator = new Mutator();
  private final Accessor accessor = new Accessor();

  public RepeatedTinyIntVector(MaterializedField field, BufferAllocator allocator) {
    super(field, allocator);
    addOrGetVector(VectorDescriptor.create(new MajorType(field.getType().getMinorType(), DataMode.REQUIRED)));
  }

  @Override
  public Mutator getMutator() {
    return mutator;
  }

  @Override
  public Accessor getAccessor() {
    return accessor;
  }

  @Override
  public FieldReader getReader() {
    return reader;
  }

  @Override
  public TinyIntVector getDataVector() {
    return values;
  }

  @Override
  public TransferPair getTransferPair(BufferAllocator allocator) {
    return new TransferImpl(getField(), allocator);
  }

  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator){
    return new TransferImpl(getField().withPath(ref), allocator);
  }

  @Override
  public TransferPair makeTransferPair(ValueVector to) {
    return new TransferImpl((RepeatedTinyIntVector) to);
  }

  @Override
  public AddOrGetResult<TinyIntVector> addOrGetVector(VectorDescriptor descriptor) {
    final AddOrGetResult<TinyIntVector> result = super.addOrGetVector(descriptor);
    if (result.isCreated()) {
      values = result.getVector();
    }
    return result;
  }

  public void transferTo(RepeatedTinyIntVector target) {
    target.clear();
    offsets.transferTo(target.offsets);
    values.transferTo(target.values);
    clear();
  }

  public void splitAndTransferTo(final int startIndex, final int groups, RepeatedTinyIntVector to) {
    final UInt4Vector.Accessor a = offsets.getAccessor();
    final UInt4Vector.Mutator m = to.offsets.getMutator();

    final int startPos = a.get(startIndex);
    final int endPos = a.get(startIndex + groups);
    final int valuesToCopy = endPos - startPos;

    values.splitAndTransferTo(startPos, valuesToCopy, to.values);
    to.offsets.clear();
    to.offsets.allocateNew(groups + 1);
    int normalizedPos = 0;
    for (int i=0; i < groups + 1;i++ ) {
      normalizedPos = a.get(startIndex+i) - startPos;
      m.set(i, normalizedPos);
    }
    m.setValueCount(groups == 0 ? 0 : groups + 1);
  }

  private class TransferImpl implements TransferPair {
    final RepeatedTinyIntVector to;

    public TransferImpl(MaterializedField field, BufferAllocator allocator) {
      this.to = new RepeatedTinyIntVector(field, allocator);
    }

    public TransferImpl(RepeatedTinyIntVector to) {
      this.to = to;
    }

    @Override
    public RepeatedTinyIntVector getTo() {
      return to;
    }

    @Override
    public void transfer() {
      transferTo(to);
    }

    @Override
    public void splitAndTransfer(int startIndex, int length) {
      splitAndTransferTo(startIndex, length, to);
    }

    @Override
    public void copyValueSafe(int fromIndex, int toIndex) {
      to.copyFromSafe(fromIndex, toIndex, RepeatedTinyIntVector.this);
    }
  }

    public void copyFrom(int inIndex, int outIndex, RepeatedTinyIntVector v) {
      final Accessor vAccessor = v.getAccessor();
      final int count = vAccessor.getInnerValueCountAt(inIndex);
      mutator.startNewValue(outIndex);
      for (int i = 0; i < count; i++) {
        mutator.add(outIndex, vAccessor.get(inIndex, i));
      }
    }

    public void copyFromSafe(int inIndex, int outIndex, RepeatedTinyIntVector v) {
      final Accessor vAccessor = v.getAccessor();
      final int count = vAccessor.getInnerValueCountAt(inIndex);
      mutator.startNewValue(outIndex);
      for (int i = 0; i < count; i++) {
        mutator.addSafe(outIndex, vAccessor.get(inIndex, i));
      }
    }

  public boolean allocateNewSafe() {
    /* boolean to keep track if all the memory allocation were successful
     * Used in the case of composite vectors when we need to allocate multiple
     * buffers for multiple vectors. If one of the allocations failed we need to
     * clear all the memory that we allocated
     */
    boolean success = false;
    try {
      if(!offsets.allocateNewSafe()) return false;
      if(!values.allocateNewSafe()) return false;
      success = true;
    } finally {
      if (!success) {
        clear();
      }
    }
    offsets.zeroVector();
    mutator.reset();
    return true;
  }

  @Override
  public void allocateNew() {
    try {
      offsets.allocateNew();
      values.allocateNew();
    } catch (OutOfMemoryException e) {
      clear();
      throw e;
    }
    offsets.zeroVector();
    mutator.reset();
  }


  @Override
  public void allocateNew(int valueCount, int innerValueCount) {
    clear();
    /* boolean to keep track if all the memory allocation were successful
     * Used in the case of composite vectors when we need to allocate multiple
     * buffers for multiple vectors. If one of the allocations failed we need to//
     * clear all the memory that we allocated
     */
    boolean success = false;
    try {
      offsets.allocateNew(valueCount + 1);
      values.allocateNew(innerValueCount);
    } catch(OutOfMemoryException e){
      clear();
      throw e;
    }
    offsets.zeroVector();
    mutator.reset();
  }


  // This is declared a subclass of the accessor declared inside of FixedWidthVector, this is also used for
  // variable length vectors, as they should ahve consistent interface as much as possible, if they need to diverge
  // in the future, the interface shold be declared in the respective value vector superclasses for fixed and variable
  // and we should refer to each in the generation template
  public final class Accessor extends BaseRepeatedValueVector.BaseRepeatedAccessor {
    @Override
    public List<Byte> getObject(int index) {
      final List<Byte> vals = new JsonStringArrayList<>();
      final UInt4Vector.Accessor offsetsAccessor = offsets.getAccessor();
      final int start = offsetsAccessor.get(index);
      final int end = offsetsAccessor.get(index + 1);
      final TinyIntVector.Accessor valuesAccessor = values.getAccessor();
      for(int i = start; i < end; i++) {
        vals.add(valuesAccessor.getObject(i));
      }
      return vals;
    }

    public Byte getSingleObject(int index, int arrayIndex) {
      final int start = offsets.getAccessor().get(index);
      return values.getAccessor().getObject(start + arrayIndex);
    }

    /**
     * Get a value for the given record.  Each element in the repeated field is accessed by
     * the positionIndex param.
     *
     * @param  index           record containing the repeated field
     * @param  positionIndex   position within the repeated field
     * @return element at the given position in the given record
     */
    public byte
            get(int index, int positionIndex) {
      return values.getAccessor().get(offsets.getAccessor().get(index) + positionIndex);
    }

    public void get(int index, RepeatedTinyIntHolder holder) {
      holder.start = offsets.getAccessor().get(index);
      holder.end =  offsets.getAccessor().get(index+1);
      holder.vector = values;
    }

    public void get(int index, int positionIndex, TinyIntHolder holder) {
      final int offset = offsets.getAccessor().get(index);
      assert offset >= 0;
      assert positionIndex < getInnerValueCountAt(index);
      values.getAccessor().get(offset + positionIndex, holder);
    }

    public void get(int index, int positionIndex, NullableTinyIntHolder holder) {
      final int offset = offsets.getAccessor().get(index);
      assert offset >= 0;
      if (positionIndex >= getInnerValueCountAt(index)) {
        holder.isSet = 0;
        return;
      }
      values.getAccessor().get(offset + positionIndex, holder);
    }
  }

  public final class Mutator extends BaseRepeatedValueVector.BaseRepeatedMutator implements RepeatedMutator {
    private Mutator() {}

    /**
     * Add an element to the given record index.  This is similar to the set() method in other
     * value vectors, except that it permits setting multiple values for a single record.
     *
     * @param index   record of the element to add
     * @param value   value to add to the given row
     */
    public void add(int index, int value) {
      int nextOffset = offsets.getAccessor().get(index+1);
      values.getMutator().set(nextOffset, value);
      offsets.getMutator().set(index+1, nextOffset+1);
    }


    public void addSafe(int index, byte srcValue) {
      final int nextOffset = offsets.getAccessor().get(index+1);
      values.getMutator().setSafe(nextOffset, srcValue);
      offsets.getMutator().setSafe(index+1, nextOffset+1);
    }


    public void setSafe(int index, RepeatedTinyIntHolder h) {
      final TinyIntHolder ih = new TinyIntHolder();
      final TinyIntVector.Accessor hVectorAccessor = h.vector.getAccessor();
      mutator.startNewValue(index);
      for(int i = h.start; i < h.end; i++){
        hVectorAccessor.get(i, ih);
        mutator.addSafe(index, ih);
      }
    }

    public void addSafe(int index, TinyIntHolder holder) {
      int nextOffset = offsets.getAccessor().get(index+1);
      values.getMutator().setSafe(nextOffset, holder);
      offsets.getMutator().setSafe(index+1, nextOffset+1);
    }

    public void addSafe(int index, NullableTinyIntHolder holder) {
      final int nextOffset = offsets.getAccessor().get(index+1);
      values.getMutator().setSafe(nextOffset, holder);
      offsets.getMutator().setSafe(index+1, nextOffset+1);
    }


    protected void add(int index, TinyIntHolder holder) {
      int nextOffset = offsets.getAccessor().get(index+1);
      values.getMutator().set(nextOffset, holder);
      offsets.getMutator().set(index+1, nextOffset+1);
    }

    public void add(int index, RepeatedTinyIntHolder holder) {

      TinyIntVector.Accessor accessor = holder.vector.getAccessor();
      TinyIntHolder innerHolder = new TinyIntHolder();

      for(int i = holder.start; i < holder.end; i++) {
        accessor.get(i, innerHolder);
        add(index, innerHolder);
      }
    }

    @Override
    public void generateTestData(final int valCount) {
      final int[] sizes = {1, 2, 0, 6};
      int size = 0;
      int runningOffset = 0;
      final UInt4Vector.Mutator offsetsMutator = offsets.getMutator();
      for(int i = 1; i < valCount + 1; i++, size++) {
        runningOffset += sizes[size % sizes.length];
        offsetsMutator.set(i, runningOffset);
      }
      values.getMutator().generateTestData(valCount * 9);
      setValueCount(size);
    }

    @Override
    public void reset() {
    }
  }
}

