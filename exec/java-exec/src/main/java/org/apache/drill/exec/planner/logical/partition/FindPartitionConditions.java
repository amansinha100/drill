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
package org.apache.drill.exec.planner.logical.partition;

import java.util.BitSet;
import java.util.List;

import org.eigenbase.rex.RexBuilder;
import org.eigenbase.rex.RexCall;
import org.eigenbase.rex.RexCorrelVariable;
import org.eigenbase.rex.RexDynamicParam;
import org.eigenbase.rex.RexFieldAccess;
import org.eigenbase.rex.RexInputRef;
import org.eigenbase.rex.RexLiteral;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexOver;
import org.eigenbase.rex.RexRangeRef;
import org.eigenbase.rex.RexVisitorImpl;
import org.eigenbase.sql.SqlKind;
import org.eigenbase.sql.SqlOperator;
import org.eigenbase.sql.fun.SqlRowOperator;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.util.Stacks;
import org.eigenbase.util.Util;

import com.google.common.collect.Lists;


public class FindPartitionConditions extends RexVisitorImpl<Void> {
  /** Whether an expression is a directory filter, and if so,
   *  whether it can be pushed into the scan
   */
  enum PushDirFilter {
    NO_PUSH, PUSH
  }

  private final BitSet dirs;

  private final List<PushDirFilter> stack =  Lists.newArrayList();
  private final List<RexNode> partitionSubTrees = Lists.newArrayList();
  private final List<SqlOperator> parentCallTypeStack = Lists.newArrayList();

  public FindPartitionConditions(BitSet dirs) {
    // go deep
    super(true);
    this.dirs = dirs;
  }

  public void analyze(RexNode exp) {
    assert stack.isEmpty();

    exp.accept(this);

    // Deal with top of stack
    assert stack.size() == 1;
    assert parentCallTypeStack.isEmpty();
    PushDirFilter rootPushDirFilter = stack.get(0);
    if (rootPushDirFilter == PushDirFilter.PUSH) {
      // The entire subtree was directory filter, so add it to the result.
      addResult(exp);
    }
    stack.clear();
  }

  public RexNode getSubTree(RexBuilder builder){
    if(partitionSubTrees.isEmpty()) {
      return null;
    }

    if(partitionSubTrees.size() == 1){
      return partitionSubTrees.get(0);
    }

    return builder.makeCall(SqlStdOperatorTable.AND, partitionSubTrees);
  }


  private Void pushVariable() {
    stack.add(PushDirFilter.NO_PUSH);
    return null;
  }

  private void addResult(RexNode exp) {
    partitionSubTrees.add(exp);
  }


  public Void visitInputRef(RexInputRef inputRef) {
    if(dirs.get(inputRef.getIndex())){
      stack.add(PushDirFilter.PUSH);
    }else{
      stack.add(PushDirFilter.NO_PUSH);
    }
    return null;
  }

  public Void visitLiteral(RexLiteral literal) {
    stack.add(PushDirFilter.PUSH);
    return null;
  }

  public Void visitOver(RexOver over) {
    // assume non-directory filter
    analyzeCall(over, PushDirFilter.NO_PUSH);
    return null;
  }

  public Void visitCorrelVariable(RexCorrelVariable correlVariable) {
    return pushVariable();
  }

  public Void visitCall(RexCall call) {
    // assume PUSH until proven otherwise
    analyzeCall(call, PushDirFilter.PUSH);
    return null;
  }

  private void analyzeCall(RexCall call, PushDirFilter callPushDirFilter) {
    Stacks.push(parentCallTypeStack, call.getOperator());

    // visit operands, pushing their states onto stack
    super.visitCall(call);

    // look for NO_PUSH operands
    int operandCount = call.getOperands().size();
    List<PushDirFilter> operandStack = Util.last(stack, operandCount);
    for (PushDirFilter operandPushDirFilter : operandStack) {
      if (operandPushDirFilter == PushDirFilter.NO_PUSH) {
        callPushDirFilter = PushDirFilter.NO_PUSH;
      }
    }

/*  These don't seem applicable to partition pruning

    // Even if all operands are PUSH, the call itself may
    // be non-deterministic.
    if (!call.getOperator().isDeterministic()) {
      callPushDirFilter = PushDirFilter.NO_PUSH;
    } else if (call.getOperator().isDynamicFunction()) {
      // We can reduce the call to a constant, but we can't
      // cache the plan if the function is dynamic.
      // For now, treat it same as non-deterministic.
      callPushDirFilter = PushDirFilter.NO_PUSH;
    }

    // Row operator itself can't be reduced to a PUSH
    if ((callPushDirFilter == PushDirFilter.PUSH)
        && (call.getOperator() instanceof SqlRowOperator)) {
      callPushDirFilter = PushDirFilter.NO_PUSH;
    }
*/

    if (callPushDirFilter == PushDirFilter.NO_PUSH && call.getKind() == SqlKind.AND) {
      // one or more children is is not a directory filter.  If this is an AND, add all the ones that are directory filters.  Otherwise, this tree cannot be pushed.
      for (int iOperand = 0; iOperand < operandCount; ++iOperand) {
        PushDirFilter PushDirFilter = operandStack.get(iOperand);
        if (PushDirFilter == PushDirFilter.PUSH) {
          addResult(call.getOperands().get(iOperand));
        }
      }
    }


    // pop operands off of the stack
    operandStack.clear();

    // pop this parent call operator off the stack
    Stacks.pop(parentCallTypeStack, call.getOperator());

    // push PushDirFilter result for this call onto stack
    stack.add(callPushDirFilter);
  }

  public Void visitDynamicParam(RexDynamicParam dynamicParam) {
    return pushVariable();
  }

  public Void visitRangeRef(RexRangeRef rangeRef) {
    return pushVariable();
  }

  public Void visitFieldAccess(RexFieldAccess fieldAccess) {
    return pushVariable();
  }


}
