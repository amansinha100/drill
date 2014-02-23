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

import net.hydromatic.optiq.rules.java.JavaRules.EnumerableTableAccessRel;

import org.apache.drill.exec.planner.common.BaseScanRel;
import org.eigenbase.relopt.Convention;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelTraitSet;

public class DrillScanRule  extends RelOptRule {
  public static final RelOptRule INSTANCE = new DrillScanRule();

  private DrillScanRule() {
    super(RelOptHelper.any(EnumerableTableAccessRel.class), "DrillTableRule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final EnumerableTableAccessRel access = (EnumerableTableAccessRel) call.rel(0);
    final RelTraitSet traits = access.getTraitSet().plus(DrillRel.DRILL_LOGICAL);
    call.transformTo(new DrillScanRel(access.getCluster(), traits, access.getTable()));
  }
}
