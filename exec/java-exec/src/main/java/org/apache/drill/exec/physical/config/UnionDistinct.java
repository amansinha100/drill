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
package org.apache.drill.exec.physical.config;

import java.util.List;

import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.exec.physical.base.AbstractMultiple;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;
import org.apache.drill.exec.proto.UserBitShared.CoreOperatorType;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("union-distinct")

public class UnionDistinct extends AbstractMultiple {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UnionDistinct.class);

  private NamedExpression[] leftChildExprs;
  private NamedExpression[] rightChildExprs;
  
  @JsonCreator
  public UnionDistinct(@JsonProperty("left-exprs") NamedExpression[] leftExprs,
      @JsonProperty("right-exprs") NamedExpression[] rightExprs, 
      @JsonProperty("children") PhysicalOperator[] children) {
    super(children);
    assert children.length == 2;
    this.leftChildExprs = leftExprs;
    this.rightChildExprs = rightExprs;
  }
  
  public NamedExpression[] getLeftChildExprs() {
    return leftChildExprs;
  }
  
  public NamedExpression[] getRightChildExprs() {
    return rightChildExprs;
  }
  
  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitUnionDistinct(this, value);
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    return new UnionDistinct(leftChildExprs, rightChildExprs, children.toArray(new PhysicalOperator[children.size()]));
  }

  @Override
  public int getOperatorType() {
    return CoreOperatorType.UNION_VALUE;
  }
}
