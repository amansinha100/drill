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
package org.apache.drill.exec.physical.impl.window;

/**
 * Used internally to keep track of partitions and frames
 */
public class Partition {
  public final int length;
  public int remaining; // rows not yet aggregated in partition
  public int peers; // peer rows not yet aggregated in current frame

  public int rowNumber;
  public int rank;
  public int denseRank;
  public double percentRank;
  public double cumeDist;

  public Partition(int length) {
    this.length = length;
    remaining = length;
    rowNumber = 1;
  }

  public void rowAggregated() {
    remaining--;
    peers--;

    rowNumber++;
  }

  public void newFrame(int peers) {
    this.peers = peers;
    rank = rowNumber; // rank = row number of 1st peer
    denseRank++;
    percentRank = length > 1 ? (double) (rank - 1) / (length - 1) : 0;
    cumeDist = (double)(rank + peers - 1) / length;
  }

  public boolean isDone() {
    return remaining == 0;
  }

  public boolean isFrameDone() {
    return peers == 0;
  }

  @Override
  public String toString() {
    return String.format("{length: %d, remaining partition: %d, remaining peers: %d}", length, remaining, peers);
  }
}
