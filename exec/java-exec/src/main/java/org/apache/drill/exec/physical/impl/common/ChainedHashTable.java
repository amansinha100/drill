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

import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.vector.*;
import org.apache.drill.exec.vector.allocator.VectorAllocator;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.TypeHelper;
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



public class ChainedHashTable implements HashTable {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ChainedHashTableTemplate.class);

  static final int[] EMPTY_TABLE = {};
  static final int NULL_SLOT = -1;

  // Array of hash buckets
  private int buckets[] = (int []) EMPTY_TABLE;

  // Container of vectors to hold type-specific key and value vectors
  private VectorContainer container;
  // Array of 'link' values 
  private int links[]; 
  
  private TypedFieldId[] keyFieldIds;
  private TypedFieldId[] valueFieldIds;
  
  // current available (free) slot
  private int freeIndex;

  private final float loadFactor;

  // Size of the hash table in terms of number of buckets
  private int tableSize; 
  // Threshold after which we rehash; It must be the tableSize * loadFactor
  private int maxFill; 
  // Actual number of entries in the hash table
  private int numEntries; 
  // Hash function to be used
  private DrillSimpleFunc hashFunc;

  public ChainedHashTable(final int initialCap,
                          final float loadf,
                          FragmentContext context,
                          NamedExpression[] keyExprs,
                          NamedExpression[] valueExprs,
                          RecordBatch incoming,
                          DrillSimpleFunc func)  {
    this.hashFunc = func;
    if (loadf <= 0 || Float.isNaN(loadf)) throw new IllegalArgumentException("Load factor must be a valid number greater than 0");
    if (initialCap <= 0) throw new IllegalArgumentException("The initial capacity must be greater than 0");
    this.loadFactor = loadf;
    
    // compute the power-of-two size of the table based on initial capacity and load factor
    tableSize = getP2Size(initialCapacity, loadf);
    maxFill = (int) Math.ceil(tableSize * loadf);
    buckets = new int[tableSize];
    // We can use the incoming batch's recordcount to size the links array. Not all positions
    // in the array may be occupied but that's ok
    links = new int[incoming.getRecordCount()]; 

    ErrorCollector collector = new ErrorCollectorImpl(); 
    keyFieldIds = new TypedFieldId[keyExprs.length];
    valueFieldIds = new TypedFieldId[valueExprs.length];
    
    for(int i = 0; i < keyExprs.length; i++){
      NamedExpression ne = keyExprs[i];
      final LogicalExpression expr =
        ExpressionTreeMaterializer.materialize(ne.getExpr(), incoming, collector);

      if(expr == null) continue;
      
      final MaterializedField outputField = MaterializedField.create(ne.getRef(), expr.getMajorType());

      // Create a type-specific ValueVector for this key
      ValueVector vector = TypeHelper.getNewVector(outputField, context.getAllocator());
      int avgBytes = 20;  // TODO: do proper calculation for variable width fields 
      VectorAllocator.getAllocator(vector, avgBytes).alloc(incoming.getRecordCount());
      keyFieldIds[i] = container.add(vector);
    }

    for(int i = 0; i < valueExprs.length; i++){
      NamedExpression ne = valueExprs[i];
      final LogicalExpression expr =
        ExpressionTreeMaterializer.materialize(ne.getExpr(), incoming, collector);

      if(expr == null) continue;
      
      final MaterializedField outputField = MaterializedField.create(ne.getRef(), expr.getMajorType());

      // Create a type-specific ValueVector for this value
      ValueVector vector = TypeHelper.getNewVector(outputField, context.getAllocator());
      int avgBytes = 20; // TODO: do proper calculation for variable width fields
      VectorAllocator.getAllocator(vector, avgBytes).alloc(incoming.getRecordCount());
      valueFieldIds[i] = container.add(vector);
    }

  }

  static int indexFor(long hash, int length) {
    return hash & (length - 1);
  }

  public int size() {
    return numEntries;
  }
  
  public boolean isEmpty() {
    return numEntries == 0;
  }

  public void clear() {

  }

  private int getStartIndex(VectorContainer key) {
    long hash = getHash(key);
    int i = hash & (buckets.length - 1);
    assert(i < buckets.length);
    return buckets[i];
  }
  
  
  private static int roundUpToPowerOf2(int number) {
    int rounded = number >= MAXIMUM_CAPACITY
           ? MAXIMUM_CAPACITY
           : (rounded = Integer.highestOneBit(number)) != 0
               ? (Integer.bitCount(number) > 1) ? rounded << 1 : rounded
               : 1;

        return rounded;
  }

  public void put(VectorContainer key, VectorContainer value) {
    if (buckets == EMPTY_TABLE) {
      inflateTable(threshold);
    }
    
    if ( checkNullKey(key) ) {
      return putForNullKey(value);
    }

    long hash = getHash(key); 
    int i = hash & (buckets.length - 1);
    assert i < buckets.length ;
    int startIdx = buckets[i];
    int currentIdx;

    if (startIdx == NULL_SLOT) {
      // this is the first entry in this bucket; find the first available slot in the 
      // container of keys and values
      currentIdx = freeIndex++;
      insertEntry(key, value, currentIdx);

      // update the buckets array
      buckets[i] = currentIdx;
      return;
    }

    currentIdx = startIdx;
    boolean found = false;
    TypedFieldId id;

    // if startIdx is non-empty, follow the hash chain links until we find a matching 
    // key or reach the end of the chain
    while (currentIdx != NULL_SLOT) {
      if (isKeyMatch(key, currentIdx)) {
        int k = 0;
        // found matching key, so just update the value 
        for (VectorWrapper<?> valueVW : value) {
          id = valueFieldIds[k++];
          setValue(id, valueVW, currentIdx);
        }
        found = true;
        break;
      }
      else {
        currentIdx = links[currentIdx];
      }
    }

    if (!found) {
      // no match was found, so insert a new entry
      currentIdx = freeIndex++;
      insertEntry(key, value, currentIdx);
    }

  }

  public VectorContainer get(VectorContainer key) {
    long hash = getHash(key);
    int i = hash & (buckets.length - 1);
    assert i < buckets.length;



  }

  private void insertEntry(VectorContainer key, VectorContainer value, int currentIdx) {
    int k = 0;
    TypedFieldId id;

    for (VectorWrapper<?> keyVW : key)  {
      id = keyFieldIds[k++];
      setValue(id, keyVW, currentIdx);
    }
    k = 0;
    for (VectorWrapper<?> valueVW: value) {
      id = valueFieldIds[k++];
      setValue(id, valueVW, currentIdx);
    }

    // update the links array; since this is the last entry in the hash chain, 
    // the links array at position currentIdx will point to a null (empty) slot
    links[currentIdx] = NULL_SLOT;
  }


  // Check if the key in the container at the currentIdx position matches the supplied key
  private boolean isKeyMatch(VectorContainer key, int currentIdx) {
    int k;
    for (VectorWrapper<?> keyVW : key) {
      TypedFieldId id = keyFieldIds[k++];
      ValueVector keyVV = keyVW.getValueVector();
      assert keyVV.getAccessor().getValueCount() == 1;

      VectorWrapper<?> containerVW = container.getValueAccessorById(id.getFieldId(), keyVW.getVectorClass());
      ValueVector containerVV = containerVW.getValueVector(); 
      MinorType mtype = containerVV.getField().getType().getMinorType();

      switch(mtype) {
      case INT:
        if ( ((IntVector) keyVV).getAccessor().get(0) == 
	     ((IntVector) containerVV).getAccessor().get(currentIdx) )
          return true;
        break;
      case BIGINT:
        if ( ((BigIntVector) keyVV).getAccessor().get(0) == 
	     ((BigIntVector) containerVV).getAccessor().get(currentIdx) ) 
          return true;
        break;
      case FLOAT4:
      	if ( ((Float4Vector) keyVV).getAccessor().get(0) == 
             ((Float4Vector) containerVV).getAccessor().get(currentIdx) )
          return true;
        break;
      case FLOAT8:
      	if ( ((Float8Vector) keyVV).getAccessor().get(0) == 
             ((Float8Vector) containerVV).getAccessor().get(currentIdx) )
          return true;
        break;
      default:
        break;
      }
    }		  
    return false;
  }

  private void setValue(TypedFieldId id, VectorWrapper<?> srcVW, int currentIdx) {
    ValueVector srcVV = srcVW.getValueVector();
    assert srcVV.getAccessor().getValueCount() == 1;

    VectorWrapper<?> containerVW = container.getValueAccessorById(id.getFieldId(), srcVW.getVectorClass());
    ValueVector containerVV = containerVW.getValueVector();
	
    MinorType mtype = containerVV.getField().getType().getMinorType();
        
    switch (mtype)  {
      case INT:
        ((IntVector) containerVV).getMutator().set(currentIdx, (int) ((IntVector)srcVV).getAccessor().get(0));
        break;
    case BIGINT:
      ((BigIntVector) containerVV).getMutator().set(currentIdx, (long) ((BigIntVector)srcVV).getAccessor().get(0));
      break;
    case FLOAT4:
      ((Float4Vector) containerVV).getMutator().set(currentIdx, (float) ((Float4Vector)srcVV).getAccessor().get(0));
      break;
    case FLOAT8:
      ((Float8Vector) containerVV).getMutator().set(currentIdx, (double) ((Float8Vector)srcVV).getAccessor().get(0));
      break;
    default:
      assert false;  // temporarily assert for now 
    }
  }

  public boolean containsKey(VectorContainer key) {
    int k;
    long hash = getHash(key);
    int i = hash & (buckets.length - 1);
    assert i < buckets.length;

    int currentIdx = buckets[i];
    
    while (currentIdx != NULL_SLOT) {
      if (isKeyMatch(key, currentIdx)) {
        return true;
      }
      currentIdx = links[currentIdx];
    }
    return false;
  }

  private long getHash(VectorContainer vc) {

  }

}

