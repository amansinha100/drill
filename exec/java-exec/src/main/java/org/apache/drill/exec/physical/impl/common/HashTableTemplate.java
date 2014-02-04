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

import javax.inject.Named;

import org.apache.drill.common.expression.ErrorCollector;
import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.allocator.VectorAllocator;

public abstract class HashTableTemplate implements HashTable { 

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HashTableTemplate.class);

  static final int EMPTY_SLOT = -1;

  // A 'bucket' consists of the start index and the hash value. 2 arrays keep track of these: 

  // Array of start indexes
  private int startIndices[] ;

  // Array of hash values - this is useful when resizing the hash table
  private int hashValues[];

  private ArrayList<BatchHolder> batchHolders;

  // Size of the hash table in terms of number of buckets
  private int tableSize; 

  // Threshold after which we rehash; It must be the tableSize * loadFactor
  private int threshold;

  // Actual number of entries in the hash table
  private int numEntries; 

  // current available (free) slot
  private int freeIndex;

  // Placeholder for the current index while probing the hash table
  private IntHolder currentIdxHolder; 

  private FragmentContext context;

  // The incoming record batch
  private RecordBatch incoming;

  // Hash table configuration parameters
  private HashTableConfig htConfig; 

  private MaterializedField[] keyFields;

  // Container of vectors to hold type-specific key and value vectors
  private VectorContainer htContainer;


  // This class encapsulates the links, keys and values for up to BATCH_SIZE
  // *unique* records. Thus, suppose there are N incoming record batches, each 
  // of size BATCH_SIZE..but they have M unique keys altogether, the number of 
  // BatchHolders will be (M/BATCH_SIZE) + 1
  private class BatchHolder {

    // Array of 'link' values 
    private int links[]; 

    private BatchHolder() {

      for(int i = 0; i < keyFields.length; i++) {
        MaterializedField outputField = keyFields[i];

        // Create a type-specific ValueVector for this key
        ValueVector vector = TypeHelper.getNewVector(outputField, context.getAllocator()) ;
        int avgBytes = 50;  // TODO: do proper calculation for variable width fields 
        VectorAllocator.getAllocator(vector, avgBytes).alloc(BATCH_SIZE) ;
        htContainer.add(vector) ;
      }

      links = new int[BATCH_SIZE];
      for (int i=0; i < links.length; i++) {
        links[i] = EMPTY_SLOT;
      }
    }

    private void setup() {
      doSetup(incoming, htContainer);
    }

    // Check if the key at the currentIdx position in hash table matches the key
    // at the incomingRowIdx. if the key does not match, update the 
    // currentIdxHolder with the index of the next link.
    private boolean isKeyMatch(int incomingRowIdx, 
                               IntHolder currentIdxHolder) {
      if (! isKeyMatchInternal(incomingRowIdx)) {
        currentIdxHolder.value = links[currentIdxHolder.value];
        return false;
      }
      return true;
    }

    // Set the values in the target value vector at specified target index using the values in the source
    // value vector at the specified source index. 
    // private void setValue(ValueVector srcVV, ValueVector targetVV, int srcIdx, int targetIdx) {

    //      TypeHelper.setValue(targetVV, targetIdx, TypeHelper.getValue(srcVV, srcIdx));
    //    }

    // Insert a new <key1, key2...keyN> entry coming from the incoming batch into the hash table 
    // container at the specified index 
    private boolean insertEntry(int incomingRowIdx, int currentIdx) { 
      setValue(incomingRowIdx, currentIdx);

      // update the links array; since this is the last entry in the hash chain, 
      // the links array at position currentIdx will point to a null (empty) slot
      links[currentIdx] = EMPTY_SLOT;

      return true;
    }

  } // class BatchHolder


  @Override
  public void setup(HashTableConfig htConfig, FragmentContext context, RecordBatch incoming)  {
    float loadf = htConfig.getLoadFactor(); 
    int initialCap = htConfig.getInitialCapacity();

    if (loadf <= 0 || Float.isNaN(loadf)) throw new IllegalArgumentException("Load factor must be a valid number greater than 0");
    if (initialCap <= 0) throw new IllegalArgumentException("The initial capacity must be greater than 0");
    if (initialCap > MAXIMUM_CAPACITY) throw new IllegalArgumentException("The initial capacity must be less than maximum capacity allowed");

    if (htConfig.getKeyExprs() == null || htConfig.getKeyExprs().length == 0) throw new IllegalArgumentException("Hash table must have at least 1 key expression");

    this.htConfig = htConfig;
    this.context = context;
    this.incoming = incoming;

    // round up the initial capacity to nearest highest power of 2
    tableSize = roundUpToPowerOf2(initialCap);
    if (tableSize > MAXIMUM_CAPACITY)
      tableSize = MAXIMUM_CAPACITY;

    threshold = (int) Math.ceil(tableSize * loadf);

    startIndices = new int[tableSize];
    hashValues = new int[tableSize];

    NamedExpression[] keyExprs = htConfig.getKeyExprs();
    keyFields = new MaterializedField[keyExprs.length];

    ErrorCollector collector = new ErrorCollectorImpl() ;
    
    for(int i = 0; i < keyExprs.length; i++) {
      NamedExpression ne = keyExprs[i] ;
      final LogicalExpression expr = 
        ExpressionTreeMaterializer.materialize(ne.getExpr(), incoming, collector, context.getFunctionRegistry()) ;

      if(expr == null) continue ;
      
      keyFields[i] = MaterializedField.create(ne.getRef(), expr.getMajorType()) ;
    }

    htContainer = new VectorContainer(); 

    // Create the first batch holder 
    batchHolders = new ArrayList<BatchHolder>();
    addBatchHolder();

    currentIdxHolder = new IntHolder();    
    initBuckets();
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

  public boolean isEmpty() {
    return numEntries == 0;
  }

  public void clear() {
    // TODO: 
  }

  private int getBucketIndex(int hash, int numBuckets) {
    return hash & (numBuckets - 1);
  }

  private static int roundUpToPowerOf2(int number) {
    int rounded = number >= MAXIMUM_CAPACITY
           ? MAXIMUM_CAPACITY
           : (rounded = Integer.highestOneBit(number)) != 0
               ? (Integer.bitCount(number) > 1) ? rounded << 1 : rounded
               : 1;

        return rounded;
  }

  public PutStatus put(int incomingRowIdx, IntHolder htIdxHolder) {

    int hash = getHash(incomingRowIdx);
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

      if (insertEntry(incomingRowIdx, currentIdx)) {
        // update the start index array
        startIndices[i] = currentIdx;
        hashValues[i] = hash;
        htIdxHolder.value = currentIdx;
        return PutStatus.KEY_ADDED;
      }
      return PutStatus.FAILED;
    }

    currentIdx = startIdx;
    boolean found = false;

    bh = batchHolders.get(currentIdx / BATCH_SIZE);
    currentIdxWithinBatch = currentIdx % BATCH_SIZE;
    currentIdxHolder.value = currentIdxWithinBatch;

    // if startIdx is non-empty, follow the hash chain links until we find a matching 
    // key or reach the end of the chain
    while (true) {
      if (bh.isKeyMatch(incomingRowIdx, currentIdxHolder)) {
        htIdxHolder.value = currentIdx;
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

      if (insertEntry(incomingRowIdx, currentIdx)) {
        htIdxHolder.value = currentIdx;
        return PutStatus.KEY_ADDED;
      }
      else 
        return PutStatus.FAILED;
    }

    return found ? PutStatus.KEY_PRESENT : PutStatus.KEY_ADDED ;
  }

  private boolean insertEntry(int incomingRowIdx, int currentIdx) {

    // resize hash table if needed and transfer contents
    resizeAndTransferIfNeeded();

    BatchHolder bh = batchHolders.get(currentIdx / BATCH_SIZE);
    int currentIdxWithinBatch = currentIdx % BATCH_SIZE;

    if (bh.insertEntry(incomingRowIdx, currentIdxWithinBatch)) {
      numEntries++ ;
      return true;
    }

    return false;
  }

  public boolean containsKey(int incomingRowIdx) {
    int hash = getHash(incomingRowIdx);
    int i = getBucketIndex(hash, numBuckets());

    int currentIdx = startIndices[i];
    
    BatchHolder bh = batchHolders.get(currentIdx / BATCH_SIZE);
    int currentIdxWithinBatch = currentIdx % BATCH_SIZE;
    currentIdxHolder.value = currentIdxWithinBatch;

    boolean found = false;

    while (true) {
      if (bh.isKeyMatch(incomingRowIdx, currentIdxHolder)) {
        found = true; 
        break;
      } else if (currentIdxHolder.value == EMPTY_SLOT) {
        break;
      } else {
        bh = batchHolders.get(currentIdxHolder.value / BATCH_SIZE);
      }
    }
   
    return found;
  }


  // Add a new BatchHolder to the list of batch holders if needed. This is based on the supplied 
  // currentIdx; since each BatchHolder can hold up to BATCH_SIZE entries, if the currentIdx exceeds
  // the capacity, we will add a new BatchHolder. 
  private void addBatchIfNeeded(int currentIdx) {
    if (currentIdx > batchHolders.size() * BATCH_SIZE) {
      addBatchHolder(); 
    }
  }

  private void addBatchHolder() {
    BatchHolder bh = new BatchHolder();
    batchHolders.add(bh);
    bh.setup();
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
    threshold = (int) Math.ceil(tableSize * htConfig.getLoadFactor());

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

  public VectorContainer getHtContainer() { 
    return htContainer;
  }

  // These methods will be code-generated 
  protected abstract void doSetup(@Named("incoming") RecordBatch incoming, 
                                  @Named("htContainer") VectorContainer htContainer);

  protected abstract boolean isKeyMatchInternal(@Named("incomingRowIdx") int incomingRowIdx);

  protected abstract void setValue(@Named("incomingRowIdx") int incomingRowIdx, 
                                   @Named("htRowIdx") int htRowIdx);

  protected abstract int getHash(@Named("incomingRowIdx") int incomingRowIdx);
  
} 


