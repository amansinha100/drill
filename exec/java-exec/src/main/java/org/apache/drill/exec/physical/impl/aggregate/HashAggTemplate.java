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
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.RecordBatch.IterOutcome;
import org.apache.drill.exec.vector.allocator.VectorAllocator;

public abstract class HashAggTemplate extends AggTemplate {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Aggregator.class);
  @Override
  public void setup(FragmentContext context, RecordBatch incoming, RecordBatch outgoing, VectorAllocator[] allocators) throws SchemaChangeException {
    super.setup(context, incoming, outgoing, allocators);
    setupInterior(incoming, outgoing);
  }

  
  @Override
  public AggOutcome doWork() {
    try{ // outside loop to ensure that first is set to false after the first run.
      
      // if we're in the first state, allocate outgoing.
      if(first){
        allocateOutgoing();
      }
      
      // pick up a remainder batch if we have one.
      if(remainderBatch != null){
        if (!outputToBatch( previousIndex )) return tooBigFailure();
        remainderBatch.clear();
        remainderBatch = null;
        return setOkAndReturn();
      }
      
      
      // setup for new output and pick any remainder.
      if (pendingOutput) {
        allocateOutgoing();
        pendingOutput = false;
        if (!outputToBatch( previousIndex)) return tooBigFailure();
      }
  
      if(newSchema){
        return AggOutcome.UPDATE_AGGREGATOR;
      }
      
      if(lastOutcome != null){
        outcome = lastOutcome;
        return AggOutcome.CLEANUP_AND_RETURN;
      }
      
      outside: while(true) {
        // loop through existing records, aggregating the values as necessary.
        for (; underlyingIndex < incoming.getRecordCount(); incIndex()) {
          if(EXTRA_DEBUG) logger.debug("Doing loop with values underlying {}, current {}", underlyingIndex, currentIndex);
          if (isGroupPresent(currentIndex)) {
            if(EXTRA_DEBUG) logger.debug("Found group-by keys in hash table, aggregating the values") ;
            aggrValuesInc(currentIndex) ;
          } else {
            if(EXTRA_DEBUG) logger.debug("Did not find group-by keys in hash table, adding keys and values");
            addGroupAndValues(currentIndex);
          }
        }

        InternalBatch previous = null;
        
        try{
          while(true){
            previous = new InternalBatch(incoming);
            IterOutcome out = incoming.next();
            if(EXTRA_DEBUG) logger.debug("Received IterOutcome of {}", out);
            switch(out){
            case NONE:
              lastOutcome = out;
              if(addedRecordCount > 0){
                if( !outputToBatchPrev( previous, previousIndex, outputCount) ) remainderBatch = previous;
                if(EXTRA_DEBUG) logger.debug("Received no more batches, returning.");
                return setOkAndReturn();
              }else{
                outcome = out;
                return AggOutcome.CLEANUP_AND_RETURN;
              }
              
            case NOT_YET:
              this.outcome = out;
              return AggOutcome.RETURN_OUTCOME;
              
            case OK_NEW_SCHEMA:
              if(EXTRA_DEBUG) logger.debug("Received new schema.  Batch has {} records.", incoming.getRecordCount());
              if(addedRecordCount > 0){
                if( !outputToBatchPrev( previous, previousIndex, outputCount) ) remainderBatch = previous;
                if(EXTRA_DEBUG) logger.debug("Wrote out end of previous batch, returning.");
                newSchema = true;
                return setOkAndReturn();
              }
              cleanup();
              return AggOutcome.UPDATE_AGGREGATOR;   
            case OK:
              resetIndex();
              if(incoming.getRecordCount() == 0){
                continue;
              } else {
                // if(isSamePrev(previousIndex , previous, currentIndex)){
                if(isGroupPresent(currentIndex)) {
                  if(EXTRA_DEBUG) logger.debug("Found group-by keys in hash table, aggregating the values");
                  aggrValuesInc(currentIndex);
                  incIndex();
                  if(EXTRA_DEBUG) logger.debug("Continuing outside loop");
                  continue outside;
                } else { // group-by keys not present
                  // if(EXTRA_DEBUG) logger.debug("This is not the same as the previous, add record and continue outside.");
                  if(EXTRA_DEBUG) logger.debug("Did not find group-by keys in hash table, adding keys and values");
                  
                  if(addedRecordCount > 0){
                    if( !outputToBatchPrev( previous, previousIndex, outputCount) ){
                      remainderBatch = previous;
                      return setOkAndReturn(); 
                    }
                    if (EXTRA_DEBUG) logger.debug("Continuing outside loop");
                    continue outside;
                  }
                }
              }
            case STOP:
            default:
              lastOutcome = out;
              outcome = out;
              return AggOutcome.CLEANUP_AND_RETURN;
            }
          }

        } finally {
          // make sure to clear previous if we haven't saved it.
          if(remainderBatch == null && previous != null) {
            previous.clear();
          }
        }
      }
    } finally{
      if(first) first = !first;
    }
  }

  private void aggrValuesInc(int index) {
    aggrValues(index);
    addedRecordCount++;
  }

  // Code-generated methods (implemented in HashAggBatch)
  public abstract boolean isGroupPresent(@Named("index") int index);
  public abstract void aggrValues(@Named("index") int index); 
  public abstract void addGroupAndValues(@Named("index") int index);
}
