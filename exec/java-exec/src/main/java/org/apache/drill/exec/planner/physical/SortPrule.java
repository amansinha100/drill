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

import org.apache.drill.exec.planner.logical.DrillSortRel;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.SortRel;
import org.eigenbase.rel.convert.ConverterRule;
import org.eigenbase.relopt.Convention;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;

/**
 * Rule that converts an {@link SortRel} to a {@link DrillSortRel}, implemented by a Drill "order" operation.
 */
public class SortPrule extends ConverterRule {
  public static final RelOptRule INSTANCE = new SortPrule();

  private SortPrule() {
    super(SortRel.class, Convention.NONE, Prel.DRILL_PHYSICAL, "SortPrule");
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    final SortRel sort = call.rel(0);
    return sort.offset == null && sort.fetch == null;
  }

  @Override
  public RelNode convert(RelNode r) {
    SortRel rel = (SortRel) r;
    return new SortPrel(rel.getCluster(), rel.getTraitSet().replace(Prel.DRILL_PHYSICAL), rel.getChild(), rel.getCollation());
  }
}
