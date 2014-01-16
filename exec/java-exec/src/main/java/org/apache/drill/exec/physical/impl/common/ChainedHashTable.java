/**
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
 */
package org.apache.drill.exec.physical.impl.common;

import java.util.ArrayList;

import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.vector.*;
import org.apache.drill.exec.vector.allocator.VectorAllocator;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.ValueVectorWriteExpression;
import org.apache.drill.exec.expr.fn.impl.ComparatorFunctions;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.AbstractRecordBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.common.expression.ErrorCollector;
import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.common.types.TypeProtos.*;

import com.google.common.hash.Hashing;
import com.google.common.hash.HashCode;


public class ChainedHashTable implements HashTable {
  // static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ChainedHashTableTemplate.class);

  static final int EMPTY_SLOT = -1;
  static final int BATCH_SIZE = Character.MAX_VALUE;

  // A 'bucket' consists of the start index and the hash value. 2 arrays keep track of these: 
  // Array of start indexes
  private int startIndices[] ;
  // Array of hash values - this is useful when resizing the hash table
  private int hashValues[];

  private ArrayList<BatchHolder> batchHolders;

  private final float loadFactor;

  // Size of the hash table in terms of number of buckets
  private int tableSize; 

  // Threshold after which we rehash; It must be the tableSize * loadFactor
  private int threshold;

  // Actual number of entries in the hash table
  private int numEntries; 

  // current available (free) slot
  private int freeIndex;

  // Hash function to be used
  // private DrillSimpleFunc hashFunc;

  // Placeholder for the current index while probing the hash table
  private IntHolder currentIdxHolder; 

  private FragmentContext context;

  // Array of keys 
  private NamedExpression[] keyExprs;
  // Array of values
  private NamedExpression[] valueExprs;
  private RecordBatch incoming;

  // This class encapsulates the links, keys and values for up to BATCH_SIZE
  // *unique* records. Thus, suppose there are N incoming record batches, each 
  // of size BATCH_SIZE..but they have M unique keys altogether, the number of 
  // BatchHolders will be M / BATCH_SIZE
  private class BatchHolder {
    // Container of vectors to hold type-specific key and value vectors
    private VectorContainer container;

    // Array of 'link' values 
    private int links[]; 
  
    private TypedFieldId[] keyFieldIds;
    private TypedFieldId[] valueFieldIds;

    private BatchHolder(FragmentContext context,
                        NamedExpression[] keyExprs,
                        NamedExpression[] valueExprs, 
                        RecordBatch incoming) {

      links = new int[BATCH_SIZE];

      this.initLinks();  

      ErrorCollector collector = new ErrorCollectorImpl() ;
      keyFieldIds = new TypedFieldId[keyExprs.length] ;
      valueFieldIds = new TypedFieldId[valueExprs.length] ;
    
      for(int i = 0; i < keyExprs.length; i++) {
        NamedExpression ne = keyExprs[i] ;
        final LogicalExpression expr = 
          ExpressionTreeMaterializer.materialize(ne.getExpr(), incoming, collector) ;

        if(expr == null) continue ;
      
        final MaterializedField outputField = MaterializedField.create(ne.getRef(), expr.getMajorType()) ;

        // Create a type-specific ValueVector for this key
        ValueVector vector = TypeHelper.getNewVector(outputField, context.getAllocator()) ;
        int avgBytes = 50;  // TODO: do proper calculation for variable width fields 
        VectorAllocator.getAllocator(vector, avgBytes).alloc(BATCH_SIZE) ;
        keyFieldIds[i] = container.add(vector) ;
      }

      for(int i = 0; i < valueExprs.length; i++) { 
        NamedExpression ne = valueExprs[i] ;
        final LogicalExpression expr = 
          ExpressionTreeMaterializer.materialize(ne.getExpr(), incoming, collector) ;

        if(expr == null) continue ;
      
        final MaterializedField outputField = MaterializedField.create(ne.getRef(), expr.getMajorType()) ;

        // Create a type-specific ValueVector for this value
        ValueVector vector = TypeHelper.getNewVector(outputField, context.getAllocator()) ;
        int avgBytes = 50; // TODO: do proper calculation for variable width fields
        VectorAllocator.getAllocator(vector, avgBytes).alloc(BATCH_SIZE) ;
        valueFieldIds[i] = container.add(vector) ;
      }
    }

    private void initLinks() {
      for (int i=0; i < links.length; i++) {
        links[i] = EMPTY_SLOT;
      }
    }
    
    // Caller has already determined that the keys match, so just update the values
    // at the specified index
    private void setValue(VectorContainer value, int idx) {
      int k = 0;
      TypedFieldId id;

      for (VectorWrapper<?> valueVW : value) {
        id = valueFieldIds[k++];
        this.setValue(id, valueVW, idx);
      }
    }

    private void setValue(TypedFieldId[] valueFieldIds, Class<?>[] valueClasses, int idx) {
      for (int i = 0; i < valueFieldIds.length; i++) {
        TypedFieldId id = valueFieldIds[i];
        Class<?> c = valueClasses[i];
        VectorWrapper<?> valueVW = incoming.getValueAccessorById(id.getFieldId(), c);
        this.setValue(id, valueVW, idx);
      }
    }

    // Get the values stored at the specified index and populate the output value container
    private void getValue(int idx, VectorContainer outValue) {
      int k = 0; 
      TypedFieldId id;

      for (VectorWrapper<?> outValueVW : outValue) {
        ValueVector outValueVV = outValueVW.getValueVector();
        id = valueFieldIds[k++];
        VectorWrapper<?> containerVW = container.getValueAccessorById(id.getFieldId(), outValueVW.getVectorClass());
        ValueVector containerVV = containerVW.getValueVector();
        
        this.setValue(containerVV, outValueVV, idx, 0);
      }
    }

    // Check if the key in the container at the currentIdx position matches the supplied key; 
    // if the key does not match, update the currentIdxHolder with the index of the next link.
    private boolean isKeyMatch(VectorContainer key, IntHolder currentIdxHolder) {
      int k = 0;
      int currentIdx = currentIdxHolder.value;
      boolean match = true;

      for (VectorWrapper<?> keyVW : key) {
        TypedFieldId id = keyFieldIds[k++];
        ValueVector keyVV = keyVW.getValueVector();
        assert keyVV.getAccessor().getValueCount() == 1;

        VectorWrapper<?> containerVW = container.getValueAccessorById(id.getFieldId(), keyVW.getVectorClass());
        ValueVector containerVV = containerVW.getValueVector(); 

        if (! TypeHelper.compareValues(keyVV, 0, containerVV, currentIdx)) {
          match = false;
          break;
        }
      }		  
      
      if (! match) { 
        // if key does not match, check the links array and update the currentIdxHolder 
        // with the index 
        currentIdxHolder.value = links[currentIdx] ;
      } 

      return match;
    }

    private boolean isKeyMatch(TypedFieldId[] keyFieldIds, Class<?>[] keyClasses,
                               IntHolder currentIdxHolder) {

      int currentIdx = currentIdxHolder.value;
      TypedFieldId id;
      Class<?> c;
      boolean match = true;

      for (int i = 0; i < keyFieldIds.length; i++) {
        id = keyFieldIds[i];
        c = keyClasses[i]; 
        VectorWrapper<?> keyVW = incoming.getValueAccessorById(id.getFieldId(), c);
        ValueVector keyVV = keyVW.getValueVector();
        assert keyVV.getAccessor().getValueCount() == 1;

        VectorWrapper<?> containerVW = container.getValueAccessorById(id.getFieldId(), keyVW.getVectorClass());
        ValueVector containerVV = containerVW.getValueVector(); 

        if ( ! TypeHelper.compareValues(keyVV, 0, containerVV, currentIdx)) {
          match = false;
          break;
        }
      }		  
    
      if (!match) {
        // if key does not match, check the links array and update the currentIdxHolder 
        // with the index 
        currentIdxHolder.value = links[currentIdx];
      }

      return match;
    }

    private void setValue(TypedFieldId id, VectorWrapper<?> srcVW, int idx) {
      ValueVector srcVV = srcVW.getValueVector();
      assert srcVV.getAccessor().getValueCount() == 1;

      VectorWrapper<?> containerVW = container.getValueAccessorById(id.getFieldId(), srcVW.getVectorClass());
      ValueVector containerVV = containerVW.getValueVector();

      this.setValue(srcVV, containerVV, 0, idx);
    }

    // Set the values in the target value vector at specified target index using the values in the source
    // value vector at the specified source index. 
    private void setValue(ValueVector srcVV, ValueVector targetVV, int srcIdx, int targetIdx) {

      TypeHelper.setValue(targetVV, targetIdx, TypeHelper.getValue(srcVV, srcIdx));
    }

    // Insert a new <key, value> entry at the specified index
    private boolean insertEntry(VectorContainer key, VectorContainer value, int idx) {
      int k = 0;
      TypedFieldId id;

      for (VectorWrapper<?> keyVW : key)  {
        id = keyFieldIds[k++];
        setValue(id, keyVW, idx);
      }
      k = 0;
      for (VectorWrapper<?> valueVW: value) {
        id = valueFieldIds[k++];
        setValue(id, valueVW, idx);
      }

      // update the links array; since this is the last entry in the hash chain, 
      // the links array at position idx will point to a null (empty) slot
      links[idx] = EMPTY_SLOT;

      return true;
    }

    // Insert a new <key, value> entry at the specified index
    private boolean insertEntry(TypedFieldId[] keyFieldIds, Class<?>[] keyClasses,
                                TypedFieldId[] valueFieldIds, Class<?>[] valueClasses, 
                                int idx) { 

      TypedFieldId id ;
      Class<?> c ;

      for (int i = 0; i < keyFieldIds.length; i++) {
        id = keyFieldIds[i];
        c = keyClasses[i];
        VectorWrapper<?> keyVW = incoming.getValueAccessorById(id.getFieldId(), c);
        setValue(id, keyVW, idx);
      }
      
      for (int i = 0; i < valueFieldIds.length; i++) {
        id = valueFieldIds[i];
        c = valueClasses[i];
        VectorWrapper<?> valueVW = incoming.getValueAccessorById(id.getFieldId(), c);
        setValue(id, valueVW, idx);
      }

      // update the links array; since this is the last entry in the hash chain, 
      // the links array at position idx will point to a null (empty) slot
      links[idx] = EMPTY_SLOT;

      return true;
    }


  } // class BatchHolder

  /** Creates a new hash table (actually a hash map of keys and values) using a customized 'chaining'
   *  strategy (the chaining is implemented not in the conventional linked list approach but rather
   *  using a 'links' array). 
   * @param initialCap the initial capacity of the hash table in terms of number of buckets.
   * @param loadf the load factor. Once this fraction of the buckets is filled, the table is resized.
   * @param context the fragment context.
   * @param keyExprs the keys of the hash map.
   * @param valueExpr the values of the hash map.
   * @param incoming the record batch of incoming records on which this hash table is being built.
   */
  public ChainedHashTable(final int initialCap,
                          final float loadf,
                          FragmentContext context,
                          NamedExpression[] keyExprs,
                          NamedExpression[] valueExprs,
                          RecordBatch incoming)  {
    // this.hashFunc = func;
    if (loadf <= 0 || Float.isNaN(loadf)) throw new IllegalArgumentException("Load factor must be a valid number greater than 0");
    if (initialCap <= 0) throw new IllegalArgumentException("The initial capacity must be greater than 0");
    if (initialCap > MAXIMUM_CAPACITY) throw new IllegalArgumentException("The initial capacity must be less than maximum capacity allowed");
    this.loadFactor = loadf;
    this.context = context;
    this.keyExprs = keyExprs;
    this.valueExprs = valueExprs;
    this.incoming = incoming;    

    // round up the initial capacity to nearest highest power of 2
    tableSize = roundUpToPowerOf2(initialCap);
    if (tableSize > MAXIMUM_CAPACITY)
      tableSize = MAXIMUM_CAPACITY;

    threshold = (int) Math.ceil(tableSize * loadf);

    startIndices = new int[tableSize];
    hashValues = new int[tableSize];

    batchHolders = new ArrayList<BatchHolder>();
    batchHolders.add( new BatchHolder(context, keyExprs, valueExprs, incoming) );

    currentIdxHolder = new IntHolder();    
    initBuckets();
  }

  public ChainedHashTable(FragmentContext context,
                          NamedExpression[] keyExprs,
                          NamedExpression[] valueExprs,
                          RecordBatch incoming)  {
    this(DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR, context, keyExprs, valueExprs, incoming);    
  }


  private void initBuckets() {
    for (int i=0; i < startIndices.length; i++) {
      startIndices[i] = EMPTY_SLOT;
    }
    for (int i=0; i < hashValues.length; i++) {
      hashValues[i] = 0;
    }
  }

  public int numBuckets() {
    return startIndices.length;
  }

  public int size() {
    return numEntries;
  }

  public float loadFactor() {
    return loadFactor;
  }
  
  public boolean isEmpty() {
    return numEntries == 0;
  }

  public void clear() {

  }

  private int getBucketIndex(int hash, int numBuckets) {
    return hash & (numBuckets - 1);
  }

 
  private int getStartIndex(VectorContainer key) {
    int hash = getHash(key);
    int i = getBucketIndex(hash, numBuckets());
    return startIndices[i];
  }

  private static int roundUpToPowerOf2(int number) {
    int rounded = number >= MAXIMUM_CAPACITY
           ? MAXIMUM_CAPACITY
           : (rounded = Integer.highestOneBit(number)) != 0
               ? (Integer.bitCount(number) > 1) ? rounded << 1 : rounded
               : 1;

        return rounded;
  }

  public boolean put(int incomingRowIdx, 
                     TypedFieldId[] keyFieldIds,  Class<?>[] keyClasses,
                     TypedFieldId[] valueFieldIds, Class<?>[] valueClasses) {

    int hash = getHash(incomingRowIdx, keyFieldIds, keyClasses); 
    int i = getBucketIndex(hash, numBuckets()); 
    int startIdx = startIndices[i];
    int currentIdx;
    int currentIdxWithinBatch;
    BatchHolder bh;

    if (startIdx == EMPTY_SLOT) {
      // this is the first entry in this bucket; find the first available slot in the 
      // container of keys and values
      currentIdx = freeIndex++;
      addBatchIfNeeded(currentIdx);

      if (insertEntry(keyFieldIds, keyClasses, valueFieldIds, valueClasses, currentIdx)) {
        // update the start index array
        startIndices[i] = currentIdx;
        hashValues[i] = hash;
        return true;
      }
      return false;
    }

    currentIdx = startIdx;
    boolean found = false;
    TypedFieldId id;

    bh = batchHolders.get(currentIdx / BATCH_SIZE);
    currentIdxWithinBatch = currentIdx % BATCH_SIZE;
    currentIdxHolder.value = currentIdxWithinBatch;

    // if startIdx is non-empty, follow the hash chain links until we find a matching 
    // key or reach the end of the chain
    while (true) {
      if (bh.isKeyMatch(keyFieldIds, keyClasses, currentIdxHolder)) {
        bh.setValue(valueFieldIds, valueClasses, currentIdxHolder.value);
        found = true;
        break;        
      }
      else if (currentIdxHolder.value == EMPTY_SLOT) {
        break;
      } else {
        bh = batchHolders.get(currentIdxHolder.value / BATCH_SIZE);
      }
    }

    if (!found) {
      // no match was found, so insert a new entry
      currentIdx = freeIndex++;
      addBatchIfNeeded(currentIdx);

      return insertEntry(keyFieldIds, keyClasses, valueFieldIds, valueClasses, currentIdx);
    }

    return found;
  }


  public boolean put(VectorContainer key, VectorContainer value) {
    // TODO: Null key processing
    //    if ( checkNullKey(key) ) {
    //      return putForNullKey(value);
    //    }

    int hash = getHash(key); 
    int i = getBucketIndex(hash, numBuckets()); 
    int startIdx = startIndices[i];
    int currentIdx;
    int currentIdxWithinBatch;
    BatchHolder bh;

    if (startIdx == EMPTY_SLOT) {
      // this is the first entry in this bucket; find the first available slot in the 
      // container of keys and values
      currentIdx = freeIndex++;
      addBatchIfNeeded(currentIdx);

      if (insertEntry(key, value, currentIdx)) {
        // update the start index array
        startIndices[i] = currentIdx;
        hashValues[i] = hash;
        return true;
      }
      return false;
    }

    currentIdx = startIdx;
    boolean found = false;
    TypedFieldId id;

    bh = batchHolders.get(currentIdx / BATCH_SIZE);
    currentIdxWithinBatch = currentIdx % BATCH_SIZE;
    currentIdxHolder.value = currentIdxWithinBatch;

    // if startIdx is non-empty, follow the hash chain links until we find a matching 
    // key or reach the end of the chain
    while (true) {
      if (bh.isKeyMatch(key, currentIdxHolder)) {
        bh.setValue(value, currentIdxHolder.value);
        found = true;
        break;        
      }
      else if (currentIdxHolder.value == EMPTY_SLOT) {
        break;
      } else {
        bh = batchHolders.get(currentIdxHolder.value / BATCH_SIZE);
      }
    }

    if (!found) {
      // no match was found, so insert a new entry
      currentIdx = freeIndex++;
      addBatchIfNeeded(currentIdx);

      return insertEntry(key, value, currentIdx);
    }

    return found;
  }

  public boolean get(VectorContainer key, VectorContainer outValue) {
    int hash = getHash(key);
    int i = getBucketIndex(hash, numBuckets());

    int startIdx = startIndices[i];

    if (startIdx == EMPTY_SLOT) {
      return false;
    }

    int currentIdx = startIdx;
    BatchHolder bh = batchHolders.get(currentIdx / BATCH_SIZE);
    int currentIdxWithinBatch = currentIdx % BATCH_SIZE;
    currentIdxHolder.value = currentIdxWithinBatch;
    boolean found = false;

    // follow the hash chain links until we find a matching key or reach the end of the chain
    while (true) {
      if (bh.isKeyMatch(key, currentIdxHolder)) {
        // populate the output value vectors
        bh.getValue(currentIdxHolder.value, outValue);
        
        found = true;
        break; 
      }
      else if (currentIdxHolder.value == EMPTY_SLOT) {
        break;
      } else {
        bh = batchHolders.get(currentIdxHolder.value / BATCH_SIZE);
      }
    }

    return found;
  }

  public boolean get(int idx, TypedFieldId[] keyFieldIds, Class<?>[] keyClasses, 
                     VectorContainer outValue) {
    int hash = getHash(idx, keyFieldIds, keyClasses); 
    int i = getBucketIndex(hash, numBuckets());

    int startIdx = startIndices[i];

    if (startIdx == EMPTY_SLOT) {
      return false;
    }

    int currentIdx = startIdx;
    BatchHolder bh = batchHolders.get(currentIdx / BATCH_SIZE);
    int currentIdxWithinBatch = currentIdx % BATCH_SIZE;
    currentIdxHolder.value = currentIdxWithinBatch;
    boolean found = false;

    // follow the hash chain links until we find a matching key or reach the end of the chain
    while (true) {
      if (bh.isKeyMatch(keyFieldIds, keyClasses, currentIdxHolder)) {
        // populate the output value vectors
        bh.getValue(currentIdxHolder.value, outValue);
        
        found = true;
        break; 
      }
      else if (currentIdxHolder.value == EMPTY_SLOT) {
        break;
      } else {
        bh = batchHolders.get(currentIdxHolder.value / BATCH_SIZE);
      }
    }

    return found;
  }

  private boolean insertEntry(VectorContainer key, VectorContainer value, int currentIdx) {

    // resize hash table if needed and transfer contents
    resizeAndTransferIfNeeded();

    BatchHolder bh = batchHolders.get(currentIdx / BATCH_SIZE);
    int currentIdxWithinBatch = currentIdx % BATCH_SIZE;
    if (bh.insertEntry(key, value, currentIdxWithinBatch)) {
      numEntries++ ;
      return true;
    }

    return false;
  }

  private boolean insertEntry(TypedFieldId[] keyFieldIds, Class<?>[] keyClasses, 
                              TypedFieldId[] valueFieldIds, Class<?>[] valueClasses, 
                              int currentIdx) { 

    // resize hash table if needed and transfer contents
    resizeAndTransferIfNeeded();

    BatchHolder bh = batchHolders.get(currentIdx / BATCH_SIZE);
    int currentIdxWithinBatch = currentIdx % BATCH_SIZE;

    if (bh.insertEntry(keyFieldIds, keyClasses, valueFieldIds, valueClasses, 
                       currentIdxWithinBatch)) {
      numEntries++ ;
      return true;
    }

    return false;
  }


  public boolean containsKey(VectorContainer key) {
    int hash = getHash(key);
    int i = getBucketIndex(hash, numBuckets());

    int currentIdx = startIndices[i];
    
    BatchHolder bh = batchHolders.get(currentIdx / BATCH_SIZE);
    int currentIdxWithinBatch = currentIdx % BATCH_SIZE;
    currentIdxHolder.value = currentIdxWithinBatch;

    while (true) {
      if (bh.isKeyMatch(key, currentIdxHolder)) {
        return true;
      } else if (currentIdxHolder.value == EMPTY_SLOT) {
        break;
      } else {
        bh = batchHolders.get(currentIdxHolder.value / BATCH_SIZE);
      }
    }

    return false;
  }

  public boolean containsKey(int idx, TypedFieldId[] keyFieldIds, Class<?>[] keyClasses) {
    int hash = getHash(idx, keyFieldIds, keyClasses);
    int i = getBucketIndex(hash, numBuckets());

    int currentIdx = startIndices[i];
    
    BatchHolder bh = batchHolders.get(currentIdx / BATCH_SIZE);
    int currentIdxWithinBatch = currentIdx % BATCH_SIZE;
    currentIdxHolder.value = currentIdxWithinBatch;

    while (true) {
      if (bh.isKeyMatch(keyFieldIds, keyClasses, currentIdxHolder)) {
        return true;
      } else if (currentIdxHolder.value == EMPTY_SLOT) {
        break;
      } else {
        bh = batchHolders.get(currentIdxHolder.value / BATCH_SIZE);
      }
    }

    return false;
  }


  // Add a new BatchHolder to the list of batch holders if needed. This is based on the supplied 
  // currentIdx; since each BatchHolder can hold up to BATCH_SIZE entries, if the currentIdx exceeds
  // the capacity, we will add a new BatchHolder. 
  private void addBatchIfNeeded(int currentIdx) {
    if (currentIdx > batchHolders.size() * BATCH_SIZE) {
      batchHolders.add( new BatchHolder(this.context, this.keyExprs, this.valueExprs, this.incoming) );
    }
  }

  // Resize the hash table if needed by creating a new one with double the number of buckets. 
  // Transfer the contents of the old hash table into the new one
  private void resizeAndTransferIfNeeded() {
    if (numEntries < threshold)
      return;

    // If the table size is already MAXIMUM_CAPACITY, don't resize 
    // the table, but set the threshold to Integer.MAX_VALUE such that 
    // future attempts to resize will return immediately. 
    if (tableSize == MAXIMUM_CAPACITY) {
      threshold = Integer.MAX_VALUE;
      return;
    }

    int newSize = 2 * tableSize;

    tableSize = roundUpToPowerOf2(newSize);
    if (tableSize > MAXIMUM_CAPACITY)
      tableSize = MAXIMUM_CAPACITY;

    // set the new threshold based on the new table size
    threshold = (int) Math.ceil(tableSize * loadFactor);

    int[] newStartIndices = new int[tableSize] ;
    int[] newHashValues = new int[tableSize];

    for (int i = 0; i < newStartIndices.length; i++) {
      newStartIndices[i] = EMPTY_SLOT;
    }
    for (int i = 0; i < newHashValues.length; i++) {
      newHashValues[i] = 0;
    }
   
    // transfer the contents of the old buckets into the new ones
    for (int oldIdx = 0; oldIdx < hashValues.length; oldIdx++) {
      int hash = hashValues[oldIdx];
      int newIdx = getBucketIndex(hash, newHashValues.length);
      newStartIndices[newIdx] = startIndices[oldIdx];
      newHashValues[newIdx] = hashValues[oldIdx];
    }

    startIndices = newStartIndices;
    hashValues = newHashValues;
  }

  private int getHash(VectorContainer vc) {
    ArrayList<HashCode> hashCodes = new ArrayList<HashCode>();
    HashCode hcode;

    for (VectorWrapper<?> vw : vc) {
      ValueVector vv = vw.getValueVector();
      assert vv.getAccessor().getValueCount() == 1;

      MinorType mtype = vv.getField().getType().getMinorType(); 

      switch(mtype) {
      case INT:
        hcode =  Hashing.murmur3_128().hashInt( ((IntVector) vv).getAccessor().get(0) );
        hashCodes.add(hcode);
        break;
      case BIGINT:
        hcode = Hashing.murmur3_128().hashLong( ((BigIntVector) vv).getAccessor().get(0) );
	      hashCodes.add(hcode);
        break;
      default:
        break;
      }
    }

    // combine the hash codes in an ordered manner
    HashCode combinedHashCode = Hashing.combineOrdered(hashCodes);
    return combinedHashCode.asInt();
  }

  private int getHash(int idx, TypedFieldId[] keyFieldIds, Class<?>[] keyClasses) {

    ArrayList<HashCode> hashCodes = new ArrayList<HashCode>();
    HashCode hcode;

    for (int i = 0; i < keyFieldIds.length; i++) {
      TypedFieldId id = keyFieldIds[i];
      Class<?> c = keyClasses[i];
      VectorWrapper<?> vw = this.incoming.getValueAccessorById(id.getFieldId(), c);

      ValueVector vv = vw.getValueVector();
      assert vv.getAccessor().getValueCount() == 1;

      MinorType mtype = vv.getField().getType().getMinorType(); 

      switch(mtype) {
      case INT:
        hcode =  Hashing.murmur3_128().hashInt( ((IntVector) vv).getAccessor().get(0) );
        hashCodes.add(hcode);
        break;
      case BIGINT:
        hcode = Hashing.murmur3_128().hashLong( ((BigIntVector) vv).getAccessor().get(0) );
	      hashCodes.add(hcode);
        break;
      default:
        break;
      }
    }

    // combine the hash codes in an ordered manner
    HashCode combinedHashCode = Hashing.combineOrdered(hashCodes);
    return combinedHashCode.asInt();
  }

}

