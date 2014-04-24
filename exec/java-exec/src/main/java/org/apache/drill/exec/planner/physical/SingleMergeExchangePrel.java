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
package org.apache.drill.exec.planner.physical;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import net.hydromatic.linq4j.Ord;

import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.logical.data.Order.Ordering;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.config.SelectionVectorRemover;
import org.apache.drill.exec.physical.config.SingleMergeExchange;
import org.apache.drill.exec.physical.config.UnionExchange;
import org.apache.drill.exec.planner.cost.DrillCostBase;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.eigenbase.rel.RelCollation;
import org.eigenbase.rel.RelFieldCollation;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.RelWriter;
import org.eigenbase.rel.SingleRel;
import org.eigenbase.rel.metadata.RelMetadataQuery;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTraitSet;

import com.beust.jcommander.internal.Lists;

public class SingleMergeExchangePrel extends SingleRel implements Prel {

  private final RelCollation collation ;

  public SingleMergeExchangePrel(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RelCollation collation) {
    super(cluster, traitSet, input);
    this.collation = collation;
    assert input.getConvention() == Prel.DRILL_PHYSICAL;
  }

  /**    
   * A SingleMergeExchange processes a total of M rows coming from N 
   * sorted input streams (from N senders) and merges them into a single 
   * output sorted stream. For costing purposes we can assume each sender
   * is sending M/N rows to a single receiver.   
   * Let 
   *   C = Cost per sender node. 
   *   s = CPU cost of Selection-Vector remover per row
   *   w = Network cost of sending 1 row to 1 destination
   *   c = CPU cost of comparing an incoming row with one on a heap of size N
   * So, C =  CPU cost of SV remover for M/N rows 
   *        + Network cost of sending M/N rows to 1 destination. 
   * So, C = (s * M/N) + (w * M/N) 
   * Cost of merging M rows coming from N senders = (M log2 N) * c
   * Total cost = N * C + (M log2 N) * c 
   */  
  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {

    RelNode child = this.getChild();
    double inputRows = RelMetadataQuery.getRowCount(child);
    int  rowWidth = child.getRowType().getPrecision();    
    double svrCpuCost = DrillCostBase.svrCpuCost * inputRows;
    double networkCost = DrillCostBase.byteNetworkCost * inputRows * rowWidth;
    int numEndPoints = 16; // hardcoded until we get it through the the planner context
    double mergeCpuCost = DrillCostBase.compareCpuCost * inputRows * (Math.log(numEndPoints)/Math.log(2));
    return new DrillCostBase(inputRows, svrCpuCost + mergeCpuCost, 0, networkCost);   
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new SingleMergeExchangePrel(getCluster(), traitSet, sole(inputs), collation);
  }

  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    Prel child = (Prel) this.getChild();

    PhysicalOperator childPOP = child.getPhysicalOperator(creator);

    if(PrelUtil.getSettings(getCluster()).isSingleMode()) return childPOP;

    //Currently, only accepts "NONE". For other, requires SelectionVectorRemover
    childPOP = PrelUtil.removeSvIfRequired(childPOP, SelectionVectorMode.NONE);

    SingleMergeExchange g = new SingleMergeExchange(childPOP, PrelUtil.getOrdering(this.collation, getChild().getRowType()));
    return g;
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    super.explainTerms(pw);
    if (pw.nest()) {
      pw.item("collation", collation);
    } else {
      for (Ord<RelFieldCollation> ord : Ord.zip(collation.getFieldCollations())) {
        pw.item("sort" + ord.i, ord.e);
      }
    }
    return pw;
  }

}
