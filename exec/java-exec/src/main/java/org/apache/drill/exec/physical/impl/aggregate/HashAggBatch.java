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
import java.util.List;

import org.apache.drill.common.expression.ErrorCollector;
import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.compile.sig.GeneratorMapping;
import org.apache.drill.exec.compile.sig.MappingSet;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.CodeGenerator.BlockType;
import org.apache.drill.exec.expr.CodeGenerator.HoldingContainer;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.expr.HoldingContainerExpression;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.expr.ValueVectorWriteExpression;
import org.apache.drill.exec.expr.fn.impl.ComparatorFunctions;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.HashAggregate;
import org.apache.drill.exec.record.AbstractRecordBatch;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.allocator.VectorAllocator;
import org.apache.drill.exec.physical.impl.aggregate.Aggregator.AggOutcome;
import org.apache.drill.exec.physical.impl.common.ChainedHashTable;
import org.apache.drill.exec.physical.impl.common.HashTable;
import org.apache.drill.exec.record.VectorWrapper;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JVar;

public class HashAggBatch extends AbstractRecordBatch<HashAggregate> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HashAggBatch.class);

  private Aggregator aggregator;
  private final RecordBatch incoming;
  private boolean done = false;
  private LogicalExpression[] groupByExprs;
  private LogicalExpression[] aggrExprs;
  private TypedFieldId[] groupByFieldIds;
  private Class<?>[] groupByClasses;
  private TypedFieldId[] aggrFieldIds;
  private Class<?>[] aggrClasses;

  // the hash table to store aggr groups (keys) and values
  private ChainedHashTable htable;
  
  public HashAggBatch(HashAggregate popConfig, RecordBatch incoming, FragmentContext context) {
    super(popConfig, context);
    this.incoming = incoming;
    float loadFactor = 0.75f;
    this.htable = new ChainedHashTable(getInitialCapacity(), loadFactor, context,
                                       popConfig.getGroupByExprs(), popConfig.getAggrExprs(),
                                       incoming);
  }

  private int getInitialCapacity() {
    // TODO: confirm if cardinality is indeed providing the estimated number of groups
    double estimatedGroups = popConfig.getCardinality();  
    return ( (estimatedGroups > (double) HashTable.MAXIMUM_CAPACITY) ? 
             HashTable.MAXIMUM_CAPACITY : (int) estimatedGroups );
  }

  @Override
  public int getRecordCount() {
    if(done) return 0;
    return aggregator.getOutputCount();
  }

  @Override
  public IterOutcome next() {

    // this is only called on the first batch. Beyond this, the aggregator manages batches.
    if (aggregator == null) {
      IterOutcome outcome = incoming.next();
      logger.debug("Next outcome of {}", outcome);
      switch (outcome) {
      case NONE:
      case NOT_YET:
      case STOP:
        return outcome;
      case OK_NEW_SCHEMA:
        if (!createAggregator()){
          done = true;
          return IterOutcome.STOP;
        }
        break;
      case OK:
        throw new IllegalStateException("You should never get a first batch without a new schema");
      default:
        throw new IllegalStateException(String.format("unknown outcome %s", outcome));
      }
    }

    while(true){
      AggOutcome out = aggregator.doWork();
      logger.debug("Aggregator response {}, records {}", out, aggregator.getOutputCount());
      switch(out){
      case CLEANUP_AND_RETURN:
        container.zeroVectors();
        done = true;
        return aggregator.getOutcome();
      case RETURN_OUTCOME:
        return aggregator.getOutcome();
      case UPDATE_AGGREGATOR:
        aggregator = null;
        if(!createAggregator()){
          return IterOutcome.STOP;
        }
        continue;
      default:
        throw new IllegalStateException(String.format("Unknown state %s.", out));
      }
    }
    
  }

  /**
   * Creates a new Aggregator based on the current schema. If setup fails, this method is responsible for cleaning up
   * and informing the context of the failure state, as well is informing the upstream operators.
   * 
   * @return true if the aggregator was setup successfully. false if there was a failure.
   */
  private boolean createAggregator() {
    logger.debug("Creating new aggregator.");
    try{
      this.aggregator = createAggregatorInternal();
      return true;
    }catch(SchemaChangeException | ClassTransformationException | IOException ex){
      context.fail(ex);
      container.clear();
      incoming.kill();
      return false;
    }
  }

  private Aggregator createAggregatorInternal() throws SchemaChangeException, ClassTransformationException, IOException{
    CodeGenerator<Aggregator> cg = new CodeGenerator<Aggregator>(AggTemplate.TEMPLATE_DEFINITION, context.getFunctionRegistry());
    container.clear();
    List<VectorAllocator> allocators = Lists.newArrayList();
    
    groupByExprs = new LogicalExpression[popConfig.getGroupByExprs().length];
    aggrExprs = new LogicalExpression[popConfig.getAggrExprs().length];
    groupByFieldIds = new TypedFieldId[popConfig.getGroupByExprs().length];
    groupByClasses = new Class<?>[popConfig.getGroupByExprs().length];
    aggrFieldIds = new TypedFieldId[popConfig.getAggrExprs().length];
    aggrClasses = new Class<?>[popConfig.getAggrExprs().length];
    
    ErrorCollector collector = new ErrorCollectorImpl();
    
    for(int i = 0; i < groupByExprs.length; i++){
      NamedExpression ne = popConfig.getGroupByExprs()[i];
      final LogicalExpression expr = ExpressionTreeMaterializer.materialize(ne.getExpr(), incoming, collector);
      if(expr == null) continue;
      groupByExprs[i] = expr;
      final MaterializedField outputField = MaterializedField.create(ne.getRef(), expr.getMajorType());
      ValueVector vector = TypeHelper.getNewVector(outputField, context.getAllocator());
      allocators.add(VectorAllocator.getAllocator(vector, 50));
      groupByFieldIds[i] = container.add(vector);

      TypeProtos.DataMode mode = expr.getMajorType().getMode(); 
      TypeProtos.MinorType mtype = expr.getMajorType().getMinorType();
      groupByClasses[i] = TypeHelper.getValueVectorClass(mtype, mode);
    }
    
    for(int i = 0; i < aggrExprs.length; i++){
      NamedExpression ne = popConfig.getAggrExprs()[i];
      final LogicalExpression expr = ExpressionTreeMaterializer.materialize(ne.getExpr(), incoming, collector);
      if(expr == null) continue;
      
      final MaterializedField outputField = MaterializedField.create(ne.getRef(), expr.getMajorType());
      ValueVector vector = TypeHelper.getNewVector(outputField, context.getAllocator());
      allocators.add(VectorAllocator.getAllocator(vector, 50));
      aggrFieldIds[i] = container.add(vector);
      aggrExprs[i] = new ValueVectorWriteExpression(aggrFieldIds[i], expr, true);

      TypeProtos.DataMode mode = expr.getMajorType().getMode(); 
      TypeProtos.MinorType mtype = expr.getMajorType().getMinorType();
      aggrClasses[i] = TypeHelper.getValueVectorClass(mtype, mode);
    }
    
    if(collector.hasErrors()) throw new SchemaChangeException("Failure while materializing expression. " + collector.toErrorString());
    
    // setupIsGroupPresent(cg, groupByExprs); 

    container.buildSchema(SelectionVectorMode.NONE);
    Aggregator agg = context.getImplementationClass(cg);
    agg.setup(context, incoming, this, allocators.toArray(new VectorAllocator[allocators.size()]));
    return agg;
  }

  /*   
  private final GeneratorMapping IS_GROUP_PRESENT = GeneratorMapping.create("setupInterior", "isGroupPresent", null, null);
  private final MappingSet IS_GROUP_PRESENT_I1 = new MappingSet("readIndex", null, IS_GROUP_PRESENT);

  // code-gen for checking if a set of group-by keys are present in the hash table
  private void setupIsGroupPresent(CodeGenerator<Aggregator> cg, LogicalExpression[] groupByExprs) {
    cg.setMappingSet(IS_GROUP_PRESENT_I1);

    for(LogicalExpression expr : groupByExprs) {
      cg.setMappingSet(IS_GROUP_PRESENT_I1);
      HoldingContainer hc = cg.addExpr(expr, false);


    }
  }
  */

  // Check if the group represented by the group-by keys at currentIndex is present 
  // in the hash table 
  private boolean isGroupPresent(int currentIndex) {
    return htable.containsKey(currentIndex, groupByFieldIds, groupByClasses);
  }
  
  private void addGroupAndValues(int currentIndex) {
    htable.put(currentIndex, groupByFieldIds, groupByClasses, aggrFieldIds, aggrClasses);
  }

  private final GeneratorMapping EVAL_INSIDE = GeneratorMapping.create("setupInterior", "aggrValues", null, null);
  private final GeneratorMapping EVAL_OUTSIDE  = GeneratorMapping.create("setupInterior", "outputRecordValues", "resetValues", "cleanup");
  private final MappingSet EVAL = new MappingSet("index", "outIndex", EVAL_INSIDE, EVAL_OUTSIDE, EVAL_INSIDE);

  private void aggrValues(CodeGenerator<Aggregator> cg, LogicalExpression[] aggrExprs) {
    cg.setMappingSet(EVAL);
    for (LogicalExpression expr : aggrExprs) {
      HoldingContainer hc = cg.addExpr(expr);
      cg.getBlock(BlockType.EVAL)._if(hc.getValue().eq(JExpr.lit(0)))._then()._return(JExpr.FALSE);
    }
    cg.getBlock(BlockType.EVAL)._return(JExpr.TRUE);
  }

  @Override
  protected void killIncoming() {
    incoming.kill();
  }

}
