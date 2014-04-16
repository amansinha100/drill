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

import org.apache.drill.exec.ops.QueryContext;
import org.eigenbase.rel.RelCollation;
import org.eigenbase.rel.RelCollationImpl;
import org.eigenbase.rel.RelCollationTraitDef;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTraitDef;

public class DrillDistributionTraitDef extends RelTraitDef<DrillDistributionTrait>{
  public static final DrillDistributionTraitDef INSTANCE = new DrillDistributionTraitDef();
  
  private QueryContext queryContext = null;
  public static int numDefaultEndPoints = 32; /* this may not be needed once we pass QueryContext
                                                  through RelOptPlanner */ 
  
  private DrillDistributionTraitDef() {
    super();
  }
  
  public boolean canConvert(
      RelOptPlanner planner, DrillDistributionTrait fromTrait, DrillDistributionTrait toTrait) {
    return true;
  }  

  public Class<DrillDistributionTrait> getTraitClass(){
    return DrillDistributionTrait.class;
  }
  
  public DrillDistributionTrait getDefault() {
    return DrillDistributionTrait.DEFAULT;
  }

  public String getSimpleName() {
    return this.getClass().getSimpleName();
  }

  public void setQueryContext(QueryContext context) {
    queryContext = context;
  }
  
  // implement RelTraitDef
  public RelNode convert(
      RelOptPlanner planner,
      RelNode rel,
      DrillDistributionTrait toDist,
      boolean allowInfiniteCostConverters) {
    
    DrillDistributionTrait currentDist = rel.getTraitSet().getTrait(DrillDistributionTraitDef.INSTANCE);
    
    // Source and Target have the same trait.
    if (currentDist.equals(toDist)) {
      return rel;
    }
    
    // Source trait is "ANY", which is abstract type of distribution.
    // We do not want to convert from "ANY", since it's abstract. 
    // Source trait should be concrete type: SINGLETON, HASH_DISTRIBUTED, etc.
    if (currentDist.equals(DrillDistributionTrait.DEFAULT)) {
      return null;
    }
    
    RelCollation collation = null;
    
    switch(toDist.getType()){
      // UnionExchange, HashToRandomExchange, OrderedPartitionExchange destroy the ordering property, therefore RelCollation is set to default, which is EMPTY.
      case SINGLETON:         
        return new UnionExchangePrel(rel.getCluster(), planner.emptyTraitSet().plus(Prel.DRILL_PHYSICAL).plus(toDist), rel);
      case HASH_DISTRIBUTED:
        return new HashToRandomExchangePrel(rel.getCluster(), planner.emptyTraitSet().plus(Prel.DRILL_PHYSICAL).plus(toDist), rel, 
                                            toDist.getFields(), this.getNumEndPoints());
      case RANGE_DISTRIBUTED:
        return new OrderedPartitionExchangePrel(rel.getCluster(), planner.emptyTraitSet().plus(Prel.DRILL_PHYSICAL).plus(toDist), rel);
      case BROADCAST_DISTRIBUTED:
        return new BroadcastExchangePrel(rel.getCluster(), planner.emptyTraitSet().plus(Prel.DRILL_PHYSICAL).plus(toDist), rel,
                                         this.getNumEndPoints());
      default:
        return null;
    }

  }
  
  // NOTE: this is a temporary function to get the number of end points until we get queryContext 
  // passed through to RelOptPlanner such that computeSelfCost() can use it
  public int getNumEndPoints() {
    if (queryContext != null) {
      return queryContext.getActiveEndpoints().size();
    }
    else {
      return numDefaultEndPoints;
    }
  }

}
