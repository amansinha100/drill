/*******************************************************************************
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
 ******************************************************************************/

package org.apache.drill.exec.planner.physical;

import java.io.IOException;
import java.util.List;

import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.config.BroadcastExchange;
import org.apache.drill.exec.physical.config.SelectionVectorRemover;
import org.apache.drill.exec.planner.cost.DrillCostBase;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.SingleRel;
import org.eigenbase.rel.metadata.RelMetadataQuery;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTraitSet;

public class BroadcastExchangePrel extends SingleRel implements Prel {

  int numEndPoints = 0;
  
  public BroadcastExchangePrel(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, 
                              int numEndPoints) {
    super(cluster, traitSet, input);
    this.numEndPoints = numEndPoints;
    assert input.getConvention() == Prel.DRILL_PHYSICAL;
  }

  /**
   * In a BroadcastExchange, each sender is sending data to N receivers (for costing
   * purposes we assume it is also sending to itself). 
   * 
   */
  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    RelNode child = this.getChild();
   
    double inputRows = RelMetadataQuery.getRowCount(child);
    int  rowWidth = child.getRowType().getPrecision();
    double cpuCost = DrillCostBase.svrCpuCost * inputRows ;
    double networkCost = DrillCostBase.byteNetworkCost * inputRows * rowWidth * numEndPoints;
    return new DrillCostBase(inputRows, cpuCost, 0, networkCost);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new BroadcastExchangePrel(getCluster(), traitSet, sole(inputs), numEndPoints);
  }
  
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    Prel child = (Prel) this.getChild();
    
    PhysicalOperator childPOP = child.getPhysicalOperator(creator);
    
    //Currently, only accepts "NONE". For other, requires SelectionVectorRemover
    if (!childPOP.getSVMode().equals(SelectionVectorMode.NONE)) {
      childPOP = new SelectionVectorRemover(childPOP);
      creator.addPhysicalOperator(childPOP);
    }


    BroadcastExchange g = new BroadcastExchange(childPOP);
    creator.addPhysicalOperator(g);
    return g;    
  }
  
}
