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

package org.apache.drill.exec.physical.impl.join;

import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;
import org.apache.drill.common.logical.data.JoinCondition;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.drill.exec.planner.logical.DrillAggregateRel;
import org.apache.drill.exec.planner.logical.DrillFilterRel;

import java.util.List;
import com.google.common.collect.Lists;

public class JoinUtils {
  public static enum JoinComparator {
    NONE, // No comparator
    EQUALS, // Equality comparator
    IS_NOT_DISTINCT_FROM // 'IS NOT DISTINCT FROM' comparator
  }

  public static enum JoinCategory {
    EQUALITY,  // equality join
    INEQUALITY,  // inequality join: <>, <, >
    CARTESIAN   // no join condition
  }

  // Check the comparator for the join condition. Note that a similar check is also
  // done in JoinPrel; however we have to repeat it here because a physical plan
  // may be submitted directly to Drill.
  public static JoinComparator checkAndSetComparison(JoinCondition condition,
      JoinComparator comparator) {
    if (condition.getRelationship().equalsIgnoreCase("EQUALS") ||
        condition.getRelationship().equals("==") /* older json plans still have '==' */) {
      if (comparator == JoinComparator.NONE ||
          comparator == JoinComparator.EQUALS) {
        return JoinComparator.EQUALS;
      } else {
        throw new IllegalArgumentException("This type of join does not support mixed comparators.");
      }
    } else if (condition.getRelationship().equalsIgnoreCase("IS_NOT_DISTINCT_FROM")) {
      if (comparator == JoinComparator.NONE ||
          comparator == JoinComparator.IS_NOT_DISTINCT_FROM) {
        return JoinComparator.IS_NOT_DISTINCT_FROM;
      } else {
        throw new IllegalArgumentException("This type of join does not support mixed comparators.");
      }
    }
    throw new IllegalArgumentException("Invalid comparator supplied to this join.");
  }

    /**
     * Check if the given RelNode contains any Cartesian join.
     * Return true if find one. Otherwise, return false.
     *
     * @param relNode   the RelNode to be inspected.
     * @param leftKeys  a list used for the left input into the join which has
     *                  equi-join keys. It can be empty or not (but not null),
     *                  this method will clear this list before using it.
     * @param rightKeys a list used for the right input into the join which has
     *                  equi-join keys. It can be empty or not (but not null),
     *                  this method will clear this list before using it.
     * @return          Return true if the given relNode contains Cartesian join.
     *                  Otherwise, return false
     */
  public static boolean checkCartesianJoin(RelNode relNode, List<Integer> leftKeys, List<Integer> rightKeys) {
    if (relNode instanceof Join) {
      leftKeys.clear();
      rightKeys.clear();

      Join joinRel = (Join) relNode;
      RelNode left = joinRel.getLeft();
      RelNode right = joinRel.getRight();

      RexNode remaining = RelOptUtil.splitJoinCondition(left, right, joinRel.getCondition(), leftKeys, rightKeys);
      if(joinRel.getJoinType() == JoinRelType.INNER) {
        if(leftKeys.isEmpty() || rightKeys.isEmpty()) {
          return true;
        }
      } else {
        if(!remaining.isAlwaysTrue() || leftKeys.isEmpty() || rightKeys.isEmpty()) {
          return true;
        }
      }
    }

    for (RelNode child : relNode.getInputs()) {
      if(checkCartesianJoin(child, leftKeys, rightKeys)) {
        return true;
      }
    }

    return false;
  }

  public static boolean isScalarSubquery(RelNode childrel) {
    DrillAggregateRel agg = null;
    RelNode currentrel = childrel;
    while (agg == null && currentrel != null) {
      if (currentrel instanceof DrillAggregateRel) {
        agg = (DrillAggregateRel)currentrel;
      } else if (currentrel instanceof DrillFilterRel) {
        currentrel = currentrel.getInput(0);
      } else if (currentrel instanceof RelSubset) {
        currentrel = ((RelSubset)currentrel).getBest() ;
      } else {
        break;
      }
    }

    if (agg != null) {
      if (agg.getGroupSet().isEmpty()) {
        return true;
      }
    }
    return false;
  }

  public static JoinCategory getJoinCategory(RelNode left, RelNode right, RexNode condition,
      List<Integer> leftKeys, List<Integer> rightKeys) {
    if (condition.isAlwaysTrue()) {
      return JoinCategory.CARTESIAN;
    }
    RexNode remaining = RelOptUtil.splitJoinCondition(left, right, condition, leftKeys, rightKeys);

    if (!remaining.isAlwaysTrue() || (leftKeys.size() == 0 || rightKeys.size() == 0) ) {
      // for practical purposes these cases could be treated as inequality
      return JoinCategory.INEQUALITY;
    }
    return JoinCategory.EQUALITY;
  }

}
