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
package org.apache.drill.exec.physical.impl.common;

import org.apache.drill.exec.compile.TemplateClassDefinition;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.VectorContainer;

public interface HashTable {

  public static TemplateClassDefinition<HashTable> TEMPLATE_DEFINITION = new TemplateClassDefinition<HashTable>(HashTable.class, HashTableTemplate.class);

  /** The initial default capacity of the hash table (in terms of number of buckets). */
  static final public int DEFAULT_INITIAL_CAPACITY = 1 << 4; 

  /** The maximum capacity of the hash table (in terms of number of buckets). */
  static final public int MAXIMUM_CAPACITY = 1 << 30; 

  /** The default load factor of a hash table. */
  final public float DEFAULT_LOAD_FACTOR = 0.75f;

  public static enum PutStatus {KEY_PRESENT, KEY_ADDED, FAILED ;}

  public static final int BATCH_SIZE = Character.MAX_VALUE;

  public void setup(HashTableConfig htConfig, FragmentContext context, RecordBatch incoming);

  public PutStatus put(int incomingRowIdx, IntHolder htIdxHolder);
  
  // public boolean get(int incomingRowIdx, VectorContainer outValue);

  public boolean containsKey(int incomingRowIdx);

  public int size();
  public boolean isEmpty();
  public void clear();

  // Ideally, we should not expose this container but the hash aggregate needs access to it 
  // in order to produce the output records...
  // TODO: explore better options that preserve encapsulation. 
  public VectorContainer getHtContainer() ;
}


