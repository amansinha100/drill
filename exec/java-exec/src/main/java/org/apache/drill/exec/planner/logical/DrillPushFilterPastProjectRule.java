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

import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.rex.RexNode;

/**
 * DrillPushFilterPastProjectRule implements the rule for pushing a {@link DrillFilterRel}
 * past a {@link DrillProjectRel}.
 */
public class DrillPushFilterPastProjectRule extends RelOptRule {
  public static final DrillPushFilterPastProjectRule INSTANCE =
      new DrillPushFilterPastProjectRule();

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a DrillPushFilterPastProjectRule.
   */
  private DrillPushFilterPastProjectRule() {
    super(
        operand(
            DrillFilterRel.class,
            operand(DrillProjectRel.class, any())));
  }

  //~ Methods ----------------------------------------------------------------

  // implement RelOptRule
  @Override
  public void onMatch(RelOptRuleCall call) {
    DrillFilterRel filterRel = call.rel(0);
    DrillProjectRel projRel = call.rel(1);

    // convert the filter to one that references the child of the project
    RexNode newCondition =
        RelOptUtil.pushFilterPastProject(filterRel.getCondition(), projRel);

    /*
    DrillFilterRel newFilterRel =
        new FilterRel(
            filterRel.getCluster(),
            projRel.getChild(),
            newCondition);
    */

    DrillFilterRel newFilterRel =
        new DrillFilterRel(filterRel.getCluster(),
            projRel.getChild().getTraitSet(),
            projRel.getChild(),
            newCondition);

    /*
    DrillProjectRel newProjRel =
        (ProjectRel) CalcRel.createProject(
            newFilterRel,
            projRel.getNamedProjects(),
            false); */

    final DrillProjectRel newProjRel =
        new DrillProjectRel(projRel.getCluster(),
            projRel.getTraitSet().plus(DrillRel.DRILL_LOGICAL),
            newFilterRel,
            projRel.getProjects(),
            projRel.getRowType());


    call.transformTo(newProjRel);
  }
}

