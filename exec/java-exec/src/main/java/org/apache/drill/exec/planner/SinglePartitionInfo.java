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
package org.apache.drill.exec.planner;


public class SinglePartitionInfo {
  private final String[] partitionValues;
  private boolean isSingle;

  public SinglePartitionInfo(int size) {
    partitionValues = new String[size];
    isSingle = true;
  }

  public boolean addPartitionValue(String value, int index) {
    if (partitionValues[index] == null) {
      partitionValues[index] = value;
      return true;
    } else if (partitionValues[index].equals(value)) {
      return true;
    } else {
      isSingle = false;
      return false;
    }
  }

  public boolean hasSinglePartition() {
    return isSingle;
  }

  public String getPath() {
    String path = "";
    for (int i = 0; i < partitionValues.length; i++) {
      if (partitionValues[i] != null) {
        path += "/" + partitionValues[i];
      }
    }
    return path;
  }

}

