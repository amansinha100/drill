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
import java.util.List;

import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.config.HashToMergeExchange;
import org.apache.drill.exec.physical.config.SelectionVectorRemover;
import org.apache.drill.exec.planner.cost.DrillCostBase;
import org.apache.drill.exec.planner.physical.DrillDistributionTrait.DistributionField;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.eigenbase.rel.RelCollation;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.SingleRel;
import org.eigenbase.rel.metadata.RelMetadataQuery;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.RelDataTypeField;


public class HashToMergeExchangePrel extends SingleRel implements Prel {

  private final List<DistributionField> distFields;
  private int numEndPoints = 0;
  private final RelCollation collation ;
  
  public HashToMergeExchangePrel(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, 
                                 List<DistributionField> fields,
                                 RelCollation collation,
                                 int numEndPoints) {
    super(cluster, traitSet, input);
    this.distFields = fields;
    this.collation = collation;
    this.numEndPoints = numEndPoints;
    assert input.getConvention() == Prel.DRILL_PHYSICAL;
  }


  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    RelNode child = this.getChild();
    double inputRows = RelMetadataQuery.getRowCount(child);

    int  rowWidth = child.getRowType().getPrecision();
    double hashCpuCost = DrillCostBase.hashCpuCost * inputRows * distFields.size();
    double svrCpuCost = DrillCostBase.svrCpuCost * inputRows;
    double mergeCpuCost = DrillCostBase.compareCpuCost * inputRows * (Math.log(numEndPoints)/Math.log(2));    
    double networkCost = DrillCostBase.byteNetworkCost * inputRows * rowWidth;
    return new DrillCostBase(inputRows, hashCpuCost + svrCpuCost + mergeCpuCost, 0, networkCost);    
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new HashToMergeExchangePrel(getCluster(), traitSet, sole(inputs), distFields, 
        this.collation, numEndPoints);
  }
  
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    Prel child = (Prel) this.getChild();
    
    PhysicalOperator childPOP = child.getPhysicalOperator(creator);
    
    //Currently, only accepts "NONE". For other, requires SelectionVectorRemover
    if (!childPOP.getSVMode().equals(SelectionVectorMode.NONE)) {
      childPOP = new SelectionVectorRemover(childPOP);
      creator.addPhysicalOperator(childPOP);
    }

    HashToMergeExchange g = new HashToMergeExchange(childPOP, 
        PrelUtil.getHashExpression(this.distFields, getChild().getRowType()),
        PrelUtil.getOrdering(this.collation, getChild().getRowType()));
    creator.addPhysicalOperator(g);
    return g;    
  }
  
  public List<DistributionField> getDistFields() {
    return this.distFields;
  }
  
  public RelCollation getCollation() {
    return this.collation;
  }
  
}
