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

import net.hydromatic.linq4j.Ord;

import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.config.HashToRandomExchange;
import org.apache.drill.exec.physical.config.SelectionVectorRemover;
import org.apache.drill.exec.planner.cost.DrillCostBase;
import org.apache.drill.exec.planner.physical.DrillDistributionTrait.DistributionField;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.RelWriter;
import org.eigenbase.rel.SingleRel;
import org.eigenbase.rel.metadata.RelMetadataQuery;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.RelDataTypeField;


public class HashToRandomExchangePrel extends SingleRel implements Prel {

  private final List<DistributionField> fields;
  private int numEndPoints = 0;
  
  public HashToRandomExchangePrel(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, List<DistributionField> fields, 
                                  int numEndPoints) {
    super(cluster, traitSet, input);
    this.fields = fields;
    this.numEndPoints = numEndPoints;
    assert input.getConvention() == Prel.DRILL_PHYSICAL;
  }

  /**
   * HashToRandomExchange processes M input rows and hash partitions them 
   * based on computing a hash value on the distribution fields. 
   * If there are N nodes (endpoints), we can assume for costing purposes 
   * on average each sender will send M/N rows to 1 destination endpoint.  
   * Let 
   *   C = Cost per node. 
   *   k = number of fields on which to distribute on
   *   h = CPU cost of computing hash value on 1 field 
   *   s = CPU cost of serializing/deserializing 
   *   w = Network cost of sending 1 row to 1 destination
   * So, C =  CPU cost of hashing k fields of M/N rows 
   *        + CPU cost of serializing/deserializing M/N rows 
   *        + Network cost of sending M/N rows to 1 destination. 
   * So, C = (h * k * M/N) + (s * M/N) + (w * M/N) 
   * Total cost = N * C
   */
  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    RelNode child = this.getChild();
    double inputRows = RelMetadataQuery.getRowCount(child);
    /* 
    int distFieldSize = 0;
    List<RelDataTypeField> distFieldList = child.getRowType().getFieldList();
    for (int i = 0; i < fields.size(); i++) {
      DistributionField f = fields.get(i);
      RelDataTypeField distField = distFieldList.get(f.getFieldId());
      distFieldSize += distField.getType().getPrecision();
    }
    */
    int  rowWidth = child.getRowType().getPrecision();
    double hashCpuCost = DrillCostBase.hashCpuCost * inputRows * fields.size();
    double serDeCpuCost = DrillCostBase.byteSerDeCpuCost * inputRows * rowWidth;
    double networkCost = DrillCostBase.byteNetworkCost * inputRows * rowWidth;
    return new DrillCostBase(inputRows, hashCpuCost + serDeCpuCost, 0, networkCost);    
    // return super.computeSelfCost(planner).multiplyBy(.1);    
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new HashToRandomExchangePrel(getCluster(), traitSet, sole(inputs), fields, numEndPoints);
  }

  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    Prel child = (Prel) this.getChild();

    PhysicalOperator childPOP = child.getPhysicalOperator(creator);

    if(PrelUtil.getSettings(getCluster()).isSingleMode()) return childPOP;

    //Currently, only accepts "NONE". For other, requires SelectionVectorRemover
    childPOP = PrelUtil.removeSvIfRequired(childPOP, SelectionVectorMode.NONE);

    HashToRandomExchange g = new HashToRandomExchange(childPOP, PrelUtil.getHashExpression(this.fields, getChild().getRowType()));
    return g;
  }

  public List<DistributionField> getFields() {
    return this.fields;
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    super.explainTerms(pw);
      for (Ord<DistributionField> ord : Ord.zip(fields)) {
        pw.item("dist" + ord.i, ord.e);
      }
    return pw;
  }

}
