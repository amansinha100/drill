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

import java.util.List;
import java.util.logging.Logger;

import org.apache.drill.exec.physical.impl.join.JoinUtils;
import org.apache.drill.exec.planner.logical.DrillJoinRel;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.eigenbase.rel.InvalidRelException;
import org.eigenbase.rel.JoinRelType;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelOptRuleOperand;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.rex.RexNode;
import org.eigenbase.sql.JoinType;
import org.eigenbase.trace.EigenbaseTrace;

import com.google.common.collect.Lists;

public class NestedLoopJoinPrule extends JoinPruleBase {
  public static final RelOptRule INSTANCE = new NestedLoopJoinPrule("Prel.NestedLoopJoinPrule", RelOptHelper.any(DrillJoinRel.class));

  protected static final Logger tracer = EigenbaseTrace.getPlannerTracer();

  private NestedLoopJoinPrule(String name, RelOptRuleOperand operand) {
    super(operand, name);
  }

  @Override
  protected boolean checkPreconditions(DrillJoinRel join, RelNode left, RelNode right,
      PlannerSettings settings) {
    JoinRelType type = join.getJoinType();

    if (! (type == JoinRelType.INNER || type == JoinRelType.LEFT)) {
      return false;
    }

    if (!join.getCondition().isAlwaysTrue()
        && (settings.isHashJoinEnabled() || settings.isMergeJoinEnabled())) {
      return false;
    }

    if (settings.isNlJoinForScalarOnly() &&
        !(JoinUtils.isScalarSubquery(left) || JoinUtils.isScalarSubquery(right))) {
      return false;
    }

    return true;
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    return PrelUtil.getPlannerSettings(call.getPlanner()).isNestedLoopJoinEnabled();
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    PlannerSettings settings = PrelUtil.getPlannerSettings(call.getPlanner());
    if (!settings.isNestedLoopJoinEnabled()) {
      return;
    }

    final DrillJoinRel join = (DrillJoinRel) call.rel(0);
    final RelNode left = join.getLeft();
    final RelNode right = join.getRight();

    if (!checkPreconditions(join, left, right, settings)) {
      return;
    }

    try {

      if (checkBroadcastConditions(call.getPlanner(), join, left, right)) {
        createBroadcastPlan(call, join, join.getCondition(), PhysicalJoinType.NESTEDLOOP_JOIN,
            left, right, null /* left collation */, null /* right collation */);
      }

    } catch (InvalidRelException e) {
      tracer.warning(e.toString());
    }
  }

}
