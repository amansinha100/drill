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
package org.apache.drill.exec.planner.logical;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.eigenbase.rel.InvalidRelException;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.UnionRel;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.trace.EigenbaseTrace;

public class DrillPushLimitPastUnionRule extends RelOptRule {
  public static final RelOptRule INSTANCE = new DrillPushLimitPastUnionRule();
  protected static final Logger tracer = EigenbaseTrace.getPlannerTracer();

  private DrillPushLimitPastUnionRule() {
    super(RelOptHelper.any(DrillLimitRel.class, UnionRel.class), "DrillPushLimitPastUnionRule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final DrillLimitRel limitRel = (DrillLimitRel) call.rel(0);
    final UnionRel union = (UnionRel) call.rel(1);
    
/*
    final RelTraitSet incomingTraits = incomingSort.getTraitSet();
    RelNode input = incomingSort.getChild();

    // if the Optiq sort rel includes a collation and a limit, we need to create a copy the sort rel that excludes the
    // limit information.
    if (!incomingSort.getCollation().getFieldCollations().isEmpty()) {
      input = incomingSort.copy(incomingTraits, input, incomingSort.getCollation(), null, null);
    }

    RelNode convertedInput = convert(input, input.getTraitSet().plus(DrillRel.DRILL_LOGICAL));
    call.transformTo(new DrillLimitRel(incomingSort.getCluster(), convertedInput.getTraitSet().plus(DrillRel.DRILL_LOGICAL), convertedInput, incomingSort.offset, incomingSort.fetch));    
*/ 
    final List<RelNode> unionNewInputs = new ArrayList<>();
    for (RelNode input : union.getInputs()) {
      // create a new Limit rel whose input is the child of the Union
      DrillLimitRel newLimitRel = new DrillLimitRel(input.getCluster(), input.getTraitSet().plus(DrillRel.DRILL_LOGICAL), 
          input, limitRel.getOffset(), limitRel.getFetch());
      unionNewInputs.add(newLimitRel);
    }
    
    call.transformTo(new DrillUnionRel(union.getCluster(), traits, unionNewInputs, union.all));
  }
}
