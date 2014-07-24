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

import org.apache.drill.common.expression.ErrorCollector;
import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.expr.ValueVectorWriteExpression;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.memory.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.UnionAll;
import org.apache.drill.exec.physical.config.UnionDistinct;
import org.apache.drill.exec.physical.impl.aggregate.HashAggregator;
import org.apache.drill.exec.physical.impl.common.ChainedHashTable;
import org.apache.drill.exec.physical.impl.common.HashTable;
import org.apache.drill.exec.physical.impl.common.HashTableConfig;
import org.apache.drill.exec.record.AbstractRecordBatch;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.RecordBatch.IterOutcome;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.vector.ValueVector;

import com.google.common.collect.Lists;
import com.sun.codemodel.JExpr;

public class UnionDistinctRecordBatch extends AbstractRecordBatch<UnionDistinct> {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UnionDistinctRecordBatch.class);

  private boolean EXTRA_DEBUG_1 = false;
  private boolean EXTRA_DEBUG_2 = false;
  
  private final List<RecordBatch> incoming;
  private SelectionVector2 sv;
  private Iterator<RecordBatch> incomingIterator = null;
  private RecordBatch current = null;
  private ArrayList<TransferPair> transfers;
  private int outRecordCount;
  private UnionDistinctOp unionDistinct;
  private NamedExpression[] leftChildExprs;
  private NamedExpression[] rightChildExprs;
  private TypedFieldId[] unionOutFieldIds;
  
  public UnionDistinctRecordBatch(UnionDistinct config, List<RecordBatch> children, FragmentContext context) throws OutOfMemoryException {
    super(config, context);
    this.incoming = children;
    this.incomingIterator = incoming.iterator();
    current = incomingIterator.next();
    sv = null;
    this.leftChildExprs = config.getLeftChildExprs();
    this.rightChildExprs = config.getRightChildExprs();
  }

  @Override
  public int getRecordCount() {
    return outRecordCount;
  }

  @Override
  public void kill(boolean sendUpstream) {
    if(current != null){
      current.kill(sendUpstream);
      current = null;
    }
    for(;incomingIterator.hasNext();){
      incomingIterator.next().kill(sendUpstream);
    }
  }

  @Override
  protected void killIncoming(boolean sendUpstream) {
    for (int i = 0; i < incoming.size(); i++) {
      RecordBatch in = incoming.get(i);
      in.kill(sendUpstream);
    }
  }


  @Override
  public SelectionVector2 getSelectionVector2() {
    return sv;
  }

  @Override
  public IterOutcome innerNext() {
    if (current == null) { // end of iteration
      return IterOutcome.NONE;
    }
    IterOutcome upstream = current.next();
    logger.debug("Upstream... {}", upstream);

    // First iterate over the child branches of the union to find a branch that has 
    // non-empty record batch
    while (upstream == IterOutcome.NONE) {
      if (!incomingIterator.hasNext()) {
        current = null;
        return IterOutcome.NONE;
      }
      current = incomingIterator.next();
      upstream = current.next();
    }
    
    switch (upstream) {
      case NONE:
        throw new IllegalArgumentException("not possible!");
      case NOT_YET:
      case STOP:
        return upstream;
      case OK_NEW_SCHEMA:
        setupSchemaAndHtable();
        // fall through.
      case OK:
        resetIndex();

        if(current.getRecordCount() == 0){
          continue;
        } else {
          boolean success = insertKeys(currentIndex);
          assert success : "UnionDistinct couldn't insert values.";
          incIndex(); 

          if(EXTRA_DEBUG_1) logger.debug("Continuing outside loop");
          continue outside;
        }
      default:
        throw new UnsupportedOperationException();
    }
  }

  private boolean createUnionDistinctOperator() {
    logger.debug("Creating new union distinct operator.");
    try{
      stats.startSetup();
      this.unionDistinct = createUnionDistinctInternal();
      return true;
    }catch(SchemaChangeException | ClassTransformationException | IOException ex){
      context.fail(ex);
      container.clear();
      incoming.kill(false);
      return false;
    }finally{
      stats.stopSetup();
    }
  }

  private UnionDistinctOp createUnionDistinctInternal() throws SchemaChangeException, ClassTransformationException, IOException{
    CodeGenerator<UnionDistinctOp> top = CodeGenerator.get(UnionDistinctOp.TEMPLATE_DEFINITION, context.getFunctionRegistry());
    ClassGenerator<UnionDistinctOp> cg = top.getRoot();

    container.clear();

    int numLeftExprs = (popConfig.getLeftChildExprs() != null) ? popConfig.getLeftChildExprs().length : 0;
    unionOutFieldIds = new TypedFieldId[numLeftExprs];

    ErrorCollector collector = new ErrorCollectorImpl();

    int i;

    for(i = 0; i < numLeftExprs; i++) {
      NamedExpression ne = popConfig.getLeftChildExprs()[i];
      final LogicalExpression expr = ExpressionTreeMaterializer.materialize(ne.getExpr(), incoming, collector, context.getFunctionRegistry() );
      if(expr == null) continue;

      final MaterializedField outputField = MaterializedField.create(ne.getRef(), expr.getMajorType());
      ValueVector vv = TypeHelper.getNewVector(outputField, oContext.getAllocator());
      unionOutFieldIds[i] = container.add(vv);
    }

    setupGetIndex(cg);
    cg.getBlock("resetValues")._return(JExpr.TRUE);

    container.buildSchema(SelectionVectorMode.NONE);
    UnionDistinctOp union = context.getImplementationClass(top);

    HashTableConfig htConfig = new HashTableConfig(context.getOptions().getOption(ExecConstants.MIN_HASH_TABLE_SIZE_KEY).num_val.intValue(),
                                                   HashTable.DEFAULT_LOAD_FACTOR,
                                                   popConfig.getLeftChildExprs(),
                                                   popConfig.getRightChildExprs()) ;
    
    union.setup(popConfig, htConfig, context, this.stats,
              oContext.getAllocator(), incoming, this,
              unionOutFieldIds, 
              this.container); 

    return union;
  }
  
  private 
  public void setupHashTable() throws IOException, SchemaChangeException, ClassTransformationException {

    // Setup the hash table configuration object

    // Set the left named expression to be null if the probe batch is empty.
    if (leftUpstream != IterOutcome.OK_NEW_SCHEMA && leftUpstream != IterOutcome.OK) {
        leftExpr = null;
    } else {
      if (left.getSchema().getSelectionVectorMode() != BatchSchema.SelectionVectorMode.NONE) {
        throw new SchemaChangeException("Hash join does not support probe batch with selection vectors");
      }
    }

    HashTableConfig htConfig =
        new HashTableConfig(context.getOptions().getOption(ExecConstants.MIN_HASH_TABLE_SIZE_KEY).num_val.intValue(),
        HashTable.DEFAULT_LOAD_FACTOR, leftChildExprs, rightChildExprs);

    // Create the chained hash table
    ChainedHashTable ht  = new ChainedHashTable(htConfig, context, oContext.getAllocator(), this.right, this.left, null);
    htable = ht.createAndSetupHashTable(null);
  }


  private void doTransfer() {
    outRecordCount = current.getRecordCount();
    if (container.getSchema().getSelectionVectorMode() == BatchSchema.SelectionVectorMode.TWO_BYTE) {
      this.sv = current.getSelectionVector2();
    }
    for (TransferPair transfer : transfers) {
      transfer.transfer();
    }

//    for (VectorWrapper<?> vw : this.container) {
//      ValueVector.Mutator m = vw.getValueVector().getMutator();
//      m.setValueCount(outRecordCount);
//    }

  }

  private void setupSchemaAndHtable() {
    if (container != null) {
      container.clear();
    }
    container.buildSchema(current.getSchema().getSelectionVectorMode());
    
    // get the current schema and build hash table for it 
    HashTableConfig htConfig = new HashTableConfig(context.getOptions().getOption(ExecConstants.MIN_HASH_TABLE_SIZE_KEY).num_val.intValue(),
        HashTable.DEFAULT_LOAD_FACTOR,
        popConfig.getLeftChildExprs(),
        popConfig.getRightChildExprs()) ;
    
    ChainedHashTable ht = new ChainedHashTable(htConfig, context, allocator, incoming, null /* no incoming probe */, this.container) ;
    this.htable = ht.createAndSetupHashTable(distOutFieldIds) ; 
    
    /*
    transfers = Lists.newArrayList();

    for (VectorWrapper<?> vw : current) {
      TransferPair pair = vw.getValueVector().getTransferPair();
      container.add(pair.getTo());
      transfers.add(pair);
    }
    container.buildSchema(current.getSchema().getSelectionVectorMode());
    */
    
  }
  
  @Override
  public WritableBatch getWritableBatch() {
    return WritableBatch.get(this);
  }
  
  @Override
  public void cleanup() {
    super.cleanup();
    for (int i = 0; i < incoming.size(); i++) {
      RecordBatch in = incoming.get(i);
      in.cleanup();
    }
  }

}
