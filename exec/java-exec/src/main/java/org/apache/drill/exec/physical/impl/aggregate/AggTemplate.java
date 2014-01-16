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
package org.apache.drill.exec.physical.impl.aggregate;

import javax.inject.Named;

import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.RecordBatch.IterOutcome;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.vector.allocator.VectorAllocator;

public abstract class AggTemplate implements Aggregator {
  protected static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Aggregator.class);
  protected static final boolean EXTRA_DEBUG = false;
  protected static final String TOO_BIG_ERROR = "Couldn't add value to an empty batch.  This likely means that a single value is too long for a varlen field.";
  protected IterOutcome lastOutcome = null;
  protected boolean first = true;
  protected boolean newSchema = false;

  protected int addedRecordCount = 0;
  protected boolean pendingOutput = false;
  protected IterOutcome outcome;
  protected int outputCount = 0;
  protected RecordBatch incoming;
  protected BatchSchema schema;
  protected RecordBatch outgoing;
  protected VectorAllocator[] allocators;
  protected FragmentContext context;
  protected InternalBatch remainderBatch;

  protected int previousIndex = 0;
  protected int underlyingIndex = 0;
  protected int currentIndex;

  @Override
  public void setup(FragmentContext context, RecordBatch incoming, RecordBatch outgoing, VectorAllocator[] allocators) throws SchemaChangeException {
    this.allocators = allocators;
    this.context = context;
    this.incoming = incoming;
    this.schema = incoming.getSchema();
    this.allocators = allocators;
    this.outgoing = outgoing;

    setupInterior(incoming, outgoing);    
    this.currentIndex = this.getVectorIndex(underlyingIndex);
  }

  
  protected void allocateOutgoing() {
    for (VectorAllocator a : allocators) {
      if(EXTRA_DEBUG) logger.debug("Allocating {} with {} records.", a, 20000);
      a.alloc(20000);
    }
  }
  
  @Override
  public IterOutcome getOutcome() {
    return outcome;
  }

  @Override
  public int getOutputCount() {
    return outputCount;
  }

  protected AggOutcome tooBigFailure(){
    context.fail(new Exception(TOO_BIG_ERROR));
    this.outcome = IterOutcome.STOP;
    return AggOutcome.CLEANUP_AND_RETURN;
  }
    
  protected final AggOutcome setOkAndReturn(){
    if(first){
      this.outcome = IterOutcome.OK_NEW_SCHEMA;
    }else{
      this.outcome = IterOutcome.OK;
    }
    for(VectorWrapper<?> v : outgoing){
      v.getValueVector().getMutator().setValueCount(outputCount);
    }
    return AggOutcome.RETURN_OUTCOME;
  }

  protected final boolean outputToBatch(int inIndex){
    boolean success = outputRecordKeys(inIndex, outputCount) //
        && outputRecordValues(outputCount) //
        && resetValues();
    if(success){
      if(EXTRA_DEBUG) logger.debug("Outputting values to {}", outputCount);
      outputCount++;
      addedRecordCount = 0;
    }
    
    return success;
  }

  protected final boolean outputToBatchPrev(InternalBatch b1, int inIndex, int outIndex){
    boolean success = outputRecordKeysPrev(b1, inIndex, outIndex) //
        && outputRecordValues(outIndex) //
        && resetValues();
    if(success){
      outputCount++;
      addedRecordCount = 0;
    }
    
    return success;
  }

  protected final void incIndex(){
    underlyingIndex++;
    if(underlyingIndex >= incoming.getRecordCount()){
      currentIndex = Integer.MAX_VALUE;
      return;
    }
    currentIndex = getVectorIndex(underlyingIndex);
  }
  
  protected final void resetIndex(){
    underlyingIndex = -1;
    incIndex();
  }

  @Override
  public void cleanup(){
    if(remainderBatch != null) remainderBatch.clear(); 
  }

  public abstract void setupInterior(@Named("incoming") RecordBatch incoming, @Named("outgoing") RecordBatch outgoing) throws SchemaChangeException;
  // public abstract void addRecord(@Named("index") int index);
  public abstract boolean outputRecordKeys(@Named("inIndex") int inIndex, @Named("outIndex") int outIndex);
  public abstract boolean outputRecordKeysPrev(@Named("previous") InternalBatch previous, @Named("previousIndex") int previousIndex, @Named("outIndex") int outIndex);
  public abstract boolean outputRecordValues(@Named("outIndex") int outIndex);
  public abstract int getVectorIndex(@Named("recordIndex") int recordIndex);
  public abstract boolean resetValues();
}
