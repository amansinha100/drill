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
      
      // TODO: Implement while loop to iterate through the existing batch records


    } finally{
      if(first) first = !first;
    }
    
  }
  
}
