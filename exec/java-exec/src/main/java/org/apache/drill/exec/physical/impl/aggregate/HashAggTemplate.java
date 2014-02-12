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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.inject.Named;

import org.apache.drill.common.expression.ErrorCollector;
import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.HashAggregate;
import org.apache.drill.exec.physical.impl.common.ChainedHashTable;
import org.apache.drill.exec.physical.impl.common.HashTable;
import org.apache.drill.exec.physical.impl.common.HashTableConfig;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.RecordBatch.IterOutcome;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.allocator.VectorAllocator;
import org.apache.drill.exec.compile.sig.RuntimeOverridden;

import com.google.common.collect.Lists;

public abstract class HashAggTemplate implements HashAggregator {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HashAggregator.class);
  
  private static final boolean EXTRA_DEBUG = true;
  private static final String TOO_BIG_ERROR = "Couldn't add value to an empty batch.  This likely means that a single value is too long for a varlen field.";
  // private IterOutcome lastOutcome = null;
  private boolean first = true;
  private boolean newSchema = false;
  private int previousIndex = 0;
  private int underlyingIndex = 0;
  private int currentIndex;
  // private int addedRecordCount = 0;
  private boolean pendingOutput = false;
  private IterOutcome outcome;
  private int outputCount = 0;
  private RecordBatch incoming;
  private BatchSchema schema;
  private RecordBatch outgoing;
  private VectorAllocator[] keyAllocators;
  private VectorAllocator[] valueAllocators;
  private FragmentContext context;
  private InternalBatch remainderBatch;

  private HashAggregate hashAggrConfig;
  private HashTable htable;
  private ArrayList<BatchHolder> batchHolders;
  private IntHolder htIdxHolder; // holder for the Hashtable's internal index returned by put()

  List<VectorAllocator> wsAllocators = Lists.newArrayList();  // allocators for the workspace vectors
  ErrorCollector collector = new ErrorCollectorImpl();
  
  private MaterializedField[] materializedValueFields;

  public class BatchHolder {

    private VectorContainer aggrValuesContainer; // container for aggr values (workspace variables)
    // TypedFieldId[] aggrFieldIds; 

    int maxOccupiedIdx = 0;

    private BatchHolder() {

      aggrValuesContainer = new VectorContainer();

      ValueVector vector ;
      
      for(int i = 0; i < materializedValueFields.length; i++) { 
        MaterializedField outputField = materializedValueFields[i];
        // Create a type-specific ValueVector for this value
        vector = TypeHelper.getNewVector(outputField, context.getAllocator()) ;
        int avgBytes = 50; // TODO: do proper calculation for variable width fields
        VectorAllocator.getAllocator(vector, avgBytes).alloc(HashTable.BATCH_SIZE) ;
        
        aggrValuesContainer.add(vector) ;
      }

    }

    private boolean updateAggrValues(int incomingRowIdx, int idxWithinBatch) {
      updateAggrValuesInternal(incomingRowIdx, idxWithinBatch);
      maxOccupiedIdx = Math.max(maxOccupiedIdx, idxWithinBatch);
      return true;
    }

    private void setup(int idx) {
      setupInterior(incoming, outgoing, aggrValuesContainer, htable.getHtContainer(idx));
    }

    private void outputRecords() { 
      for (int i = 0; i <= maxOccupiedIdx; i++) { 
        if (outputRecordValues(outputCount, i)) {
          // if (outputAllRecords(outputCount, i)) {
          if (EXTRA_DEBUG) logger.debug("Outputting values to {}", outputCount) ;
          outputCount++;
        }
      }
    }

    // Code-generated methods (implemented in HashAggBatch)

    @RuntimeOverridden
    public void setupInterior(@Named("incoming") RecordBatch incoming, @Named("outgoing") RecordBatch outgoing, @Named("aggrValuesContainer") VectorContainer aggrValuesContainer, @Named("htContainer") VectorContainer htContainer) {}

    @RuntimeOverridden
    public void updateAggrValuesInternal(@Named("incomingRowIdx") int incomingRowIdx, @Named("htRowIdx") int htRowIdx) {}

    @RuntimeOverridden
    public boolean outputRecordValues(@Named("outRowIdx") int outRowIdx, @Named("htRowIdx") int htRowIdx) {return true;} 

    // @RuntimeOverridden
    // public boolean outputAllRecords(@Named("outRowIdx") int outRowIdx, @Named("htRowIdx") int htRowIdx) {return false;} 

  }


  @Override
  public void setup(HashAggregate hashAggrConfig, FragmentContext context, RecordBatch incoming, RecordBatch outgoing, 
                    LogicalExpression[] valueExprs, 
                    ArrayList<TypedFieldId> valueFieldIds, 
                    VectorAllocator[] keyAllocators, VectorAllocator[] valueAllocators) 
    throws SchemaChangeException, ClassTransformationException, IOException {

    if (valueFieldIds.size() < valueExprs.length) throw new IllegalArgumentException("Wrong number of workspace variables.");

    this.context = context;
    this.incoming = incoming;
    this.schema = incoming.getSchema();
    this.keyAllocators = keyAllocators;
    this.valueAllocators = valueAllocators;
    this.outgoing = outgoing;
    
    this.hashAggrConfig = hashAggrConfig;

    this.htIdxHolder = new IntHolder(); 
    materializedValueFields = new MaterializedField[valueFieldIds.size()];

    int i = 0;
    FieldReference ref = new FieldReference("dummy", ExpressionPosition.UNKNOWN, valueFieldIds.get(0).getType());
    for (TypedFieldId id : valueFieldIds) {
      materializedValueFields[i++] = MaterializedField.create(ref, id.getType());
    }

    ChainedHashTable ht = new ChainedHashTable(hashAggrConfig.getHtConfig(), context, incoming);
    this.htable = ht.createAndSetupHashTable();

    batchHolders = new ArrayList<BatchHolder>();
    addBatchHolder(); 
  }

  @Override
  public AggOutcome doWork() {
    try{ // outside loop to ensure that first is set to false after the first run.
      
      // if we're in the first state, allocate outgoing.
      if(first){
        allocateOutgoing();
      }

      /*       
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
      */
  
      if(newSchema){
        return AggOutcome.UPDATE_AGGREGATOR;
      }
      
      // if(lastOutcome != null){
      //  outcome = lastOutcome;
      //  return AggOutcome.CLEANUP_AND_RETURN;
      // }
      
      outside: while(true) {
        // loop through existing records, aggregating the values as necessary.
        for (; underlyingIndex < incoming.getRecordCount(); incIndex()) {
          if(EXTRA_DEBUG) logger.debug("Doing loop with values underlying {}, current {}", underlyingIndex, currentIndex);
          checkGroupAndAggrValues(currentIndex); 
        }

        // InternalBatch previous = null;
        
        try{
          while(true){
            // previous = new InternalBatch(incoming);

            IterOutcome out = incoming.next();
            if(EXTRA_DEBUG) logger.debug("Received IterOutcome of {}", out);
            switch(out){
            case NOT_YET:
              this.outcome = out;
              return AggOutcome.RETURN_OUTCOME;
              
            case OK_NEW_SCHEMA:
              if(EXTRA_DEBUG) logger.debug("Received new schema.  Batch has {} records.", incoming.getRecordCount());
              // if(addedRecordCount > 0){
              //  if( !outputToBatchPrev( previous, previousIndex, outputCount) ) remainderBatch = previous;
              //  if(EXTRA_DEBUG) logger.debug("Wrote out end of previous batch, returning.");

              //  newSchema = true;
              //  return setOkAndReturn();

                // }

              cleanup();
              return AggOutcome.UPDATE_AGGREGATOR;   
            case OK:
              // resetIndex();
              if(incoming.getRecordCount() == 0){
                continue;
              } else {
                checkGroupAndAggrValues(currentIndex);
                incIndex();

                if(EXTRA_DEBUG) logger.debug("Continuing outside loop");
                continue outside;
              }

            case NONE:
              outcome = out;
              outputRecordValues() ; 
              return setOkAndReturn();
            case STOP:
            default:
              outcome = out;
              return AggOutcome.CLEANUP_AND_RETURN;
            }
          }

        } finally {
          // make sure to clear previous if we haven't saved it.
          //if(remainderBatch == null && previous != null) {
          //   previous.clear();
          // }
        }
      }
    } finally{
      if(first) first = !first;
    }
  }

  private void allocateOutgoing() {
    for (VectorAllocator a : keyAllocators) {
      if(EXTRA_DEBUG) logger.debug("Allocating {} with {} records.", a, 20000);
      a.alloc(20000);
    }

    for (VectorAllocator a : valueAllocators) {
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

  @Override
  public void cleanup(){
    // TODO: 
  }



  private AggOutcome tooBigFailure(){
    context.fail(new Exception(TOO_BIG_ERROR));
    this.outcome = IterOutcome.STOP;
    return AggOutcome.CLEANUP_AND_RETURN;
  }
  
  private final AggOutcome setOkAndReturn(){
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

  private final void incIndex(){
    underlyingIndex++;
    if(underlyingIndex >= incoming.getRecordCount()){
      currentIndex = Integer.MAX_VALUE;
      return;
    }
    currentIndex = getVectorIndex(underlyingIndex);
  }
  
  private final void resetIndex(){
    underlyingIndex = -1;
    incIndex();
  }

  private void addBatchHolder() {
    BatchHolder bh = new BatchHolder(); 
    batchHolders.add(bh);
    int batchIdx = batchHolders.size() - 1;

    bh.setup(batchIdx); 
  }

  private void outputRecordValues() {

    for (BatchHolder bh : batchHolders) {
      bh.outputRecords();
    }
  }

  // Check if a group is present in the hash table; if not, insert it in the hash table. 
  // The htIdxHolder contains the index of the group in the hash table container; this same 
  // index is also used for the aggregation values maintained by the hash aggregate. 
  private boolean checkGroupAndAggrValues(int incomingRowIdx) {
    
    HashTable.PutStatus putStatus = htable.put(incomingRowIdx, htIdxHolder) ;

    if (putStatus != HashTable.PutStatus.PUT_FAILED) {
      int currentIdx = htIdxHolder.value;

      // get the batch index and index within the batch
      if (currentIdx > batchHolders.size() * HashTable.BATCH_SIZE) {
        addBatchHolder();
      }
      BatchHolder bh = batchHolders.get(currentIdx / HashTable.BATCH_SIZE);
      int idxWithinBatch = currentIdx % HashTable.BATCH_SIZE;

      if (putStatus == HashTable.PutStatus.KEY_PRESENT) {
        if (EXTRA_DEBUG) logger.debug("Group-by key already present in hash table, updating the aggregate values");
      } 
      else if (putStatus == HashTable.PutStatus.KEY_ADDED) {
        if (EXTRA_DEBUG) logger.debug("Group-by key was added to hash table, inserting new aggregate values") ;
      }
      
      return bh.updateAggrValues(incomingRowIdx, idxWithinBatch);
      
    } 

    return false;
  }
 
  // Code-generated methods (implemented in HashAggBatch)
  public abstract int getVectorIndex(@Named("recordIndex") int recordIndex);
  public abstract boolean resetValues();

}
