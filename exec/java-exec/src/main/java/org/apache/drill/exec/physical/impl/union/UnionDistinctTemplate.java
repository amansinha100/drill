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
package org.apache.drill.exec.physical.impl.union;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.drill.exec.compile.TemplateClassDefinition;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.MetricDef;
import org.apache.drill.exec.ops.OperatorStats;
import org.apache.drill.exec.physical.config.UnionDistinct;
import org.apache.drill.exec.physical.impl.aggregate.HashAggBatch;
import org.apache.drill.exec.physical.impl.common.ChainedHashTable;
import org.apache.drill.exec.physical.impl.common.HashTable;
import org.apache.drill.exec.physical.impl.common.HashTableConfig;
import org.apache.drill.exec.physical.impl.common.HashTableStats;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.RecordBatch.IterOutcome;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.vector.ValueVector;

public abstract class UnionDistinctTemplate implements UnionDistinctOp {
  public static TemplateClassDefinition<UnionDistinctOp> TEMPLATE_DEFINITION = new TemplateClassDefinition<UnionDistinctOp>(UnionDistinctOp.class, UnionDistinctTemplate.class);

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UnionDistinctOp.class);

  private static final long ALLOCATOR_INITIAL_RESERVATION = 1*1024*1024;
  private static final long ALLOCATOR_MAX_RESERVATION = 20L*1000*1000*1000;

  private static final boolean EXTRA_DEBUG_1 = false;
  private static final boolean EXTRA_DEBUG_2 = false;
  private boolean first = true;
  private int underlyingIndex = 0;
  private int currentIndex = 0;
  private IterOutcome outcome;
  private int outputCount = 0;
  private int numGroupedRecords = 0;
  private int outBatchIndex = 0;
  private int lastBatchOutputCount = 0;
  private List<RecordBatch> incomingBatches;
  private UnionDistinctRecordBatch outgoing;
  private BatchSchema schema;
  private VectorContainer outContainer;
  private FragmentContext context;
  private BufferAllocator allocator;
  private UnionDistinct unionDistConfig;
  private HashTable htable;
  private IntHolder htIdxHolder;
  private int numDistOutFields;
  private boolean buildComplete = false;
  private long numDistinctRecords = 0;
  
  private OperatorStats stats = null;
  private HashTableStats htStats = new HashTableStats();
  
  public enum Metric implements MetricDef {

    NUM_BUCKETS,
    NUM_ENTRIES,
    NUM_RESIZING,
    RESIZING_TIME;

    @Override
    public int metricId() {
      return ordinal();
    }
  }    
  
  @Override
  public void setup(UnionDistinct unionDistConfig, HashTableConfig htConfig, 
                    FragmentContext context, 
                    OperatorStats stats,
                    BufferAllocator allocator,
                    List<RecordBatch> incomingBatches, 
                    UnionDistinctRecordBatch outgoing,
                    TypedFieldId[] distOutFieldIds, 
                    VectorContainer outContainer) 
    throws SchemaChangeException, ClassTransformationException, IOException {

    this.context = context;
    this.stats = stats;
    this.incoming = incomingBatches;
    this.schema = incoming.getSchema();
    this.outgoing = outgoing;
    this.outContainer = outContainer;

    this.unionDistConfig = unionDistConfig;
    this.htIdxHolder = new IntHolder();
    ChainedHashTable ht = new ChainedHashTable(htConfig, context, allocator, incoming, null /* no incoming probe */, outgoing) ;
    this.htable = ht.createAndSetupHashTable(distOutFieldIds) ;

    numDistOutFields = distOutFieldIds.length;

    doSetup(incoming);
  }

  @Override
  public UnionOutcome doWork() {
    try{
      // Note: Keeping the outer and inner try blocks here to maintain some similarity with
      // StreamingAggregate which does somethings conditionally in the outer try block.
      // In the future HashAggregate may also need to perform some actions conditionally
      // in the outer try block.

      outside: while(true) {
        // loop through existing records, aggregating the values as necessary.
        if (EXTRA_DEBUG_1) logger.debug ("Starting outer loop of doWork()...");
        for (; underlyingIndex < incoming.getRecordCount(); incIndex()) {
          if(EXTRA_DEBUG_2) logger.debug("Doing loop with values underlying {}, current {}", underlyingIndex, currentIndex);
          boolean success = checkGroupAndUnionValues(currentIndex);
          assert success : "UnionDistinct couldn't copy values.";
        }

        if (EXTRA_DEBUG_1) logger.debug("Processed {} records", underlyingIndex);

        try{

          while(true){
            // Cleanup the previous batch since we are done processing it.
            for (VectorWrapper<?> v : incoming) {
              v.getValueVector().clear();
            }
            IterOutcome out = outgoing.next(0, incoming);
            if(EXTRA_DEBUG_1) logger.debug("Received IterOutcome of {}", out);
            switch(out){
            case NOT_YET:
              this.outcome = out;
              return UnionOutcome.RETURN_OUTCOME;

            case OK_NEW_SCHEMA:
              if(EXTRA_DEBUG_1) logger.debug("Received new schema.  Batch has {} records.", incoming.getRecordCount());
              newSchema = true;
              this.cleanup();
              // TODO: new schema case needs to be handled appropriately
              return UnionOutcome.UPDATE_UNION;

            case OK:
              resetIndex();
              if(incoming.getRecordCount() == 0){
                continue;
              } else {
                boolean success = checkGroupAndUnionValues(currentIndex);
                assert success : "HashAgg couldn't copy values.";
                incIndex();

                if(EXTRA_DEBUG_1) logger.debug("Continuing outside loop");
                continue outside;
              }

            case NONE:
              // outcome = out;

              buildComplete = true;
              
              updateStats(htable);

              // output the first batch; remaining batches will be output
              // in response to each next() call by a downstream operator

              outputCurrentBatch();

              // cleanup incoming batch since output of aggregation does not need
              // any references to the incoming

              incoming.cleanup();
              // return setOkAndReturn();
              return UnionOutcome.RETURN_OUTCOME;

            case STOP:
            default:
              outcome = out;
              return UnionOutcome.CLEANUP_AND_RETURN;
            }
          }

        } finally {
          // placeholder...
        }
      }
    } finally{
      if(first) first = !first;
    }
  }
  
  private boolean insertKeys(int incomingRowIdx) {
    if (incomingRowIdx < 0) {
      throw new IllegalArgumentException("Invalid incoming row index.");
    }

    HashTable.PutStatus putStatus = htable.put(incomingRowIdx, htIdxHolder, 1 /* retry count */) ;

    if (putStatus != HashTable.PutStatus.PUT_FAILED) {
      if (putStatus == HashTable.PutStatus.KEY_PRESENT) {
        if (EXTRA_DEBUG_2) logger.debug("Key already present in hash table.");
      }
      else if (putStatus == HashTable.PutStatus.KEY_ADDED) {
        if (EXTRA_DEBUG_2) logger.debug("Key was added to hash table.") ;
      }

      numDistinctRecords++;
      return true;
    }
    
    logger.debug("UnionDistinct Put failed ! incomingRowIdx = {}, hash table size = {}.", incomingRowIdx, htable.size());
    return false;
  }


  private void allocateOutgoing() {
    // Skip the keys and only allocate for outputting the workspace values
    // (keys will be output through splitAndTransfer)
    Iterator<VectorWrapper<?>> outgoingIter = outContainer.iterator();
    for (int i=0; i < numDistOutFields; i++) {
      outgoingIter.next();
    }
    while (outgoingIter.hasNext()) {
      ValueVector vv = outgoingIter.next().getValueVector();
      vv.allocateNew();
    }
  }
}

