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

import org.apache.drill.exec.compile.TemplateClassDefinition;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.VectorContainer;

import java.util.List;

public interface NestedLoopJoin {
  public static TemplateClassDefinition<NestedLoopJoin> TEMPLATE_DEFINITION =
      new TemplateClassDefinition<>(NestedLoopJoin.class, NestedLoopJoinTemplate.class);

  public void setupNestedLoopJoin(FragmentContext context, VectorContainer right, List<Integer> rightCounts, RecordBatch left, NestedLoopJoinBatch outgoing);
  public int outputRecords();
  public void emitRight(int rightCompositeIndex, int outIndex);
  public void emitLeft(int leftIndex, int outIndex);
  public void doSetup(FragmentContext context, VectorContainer rightContainer, RecordBatch leftBatch, RecordBatch outgoing);
}
