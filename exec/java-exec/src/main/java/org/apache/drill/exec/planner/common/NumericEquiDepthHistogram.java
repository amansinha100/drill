/*
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
package org.apache.drill.exec.planner.common;


import org.apache.calcite.rex.RexNode;

import com.clearspring.analytics.stream.quantile.TDigest;

/**
 * A column specific equi-depth histogram which is meant for numeric data types
 */
public class NumericEquiDepthHistogram implements Histogram {
  public static final int NUM_BUCKETS = 32;

  private static class Bucket {
    private double start;
    private double end;
    // TODO: add ndv value per bucket once it is available
    // private double ndv ;
    public Bucket() {
      start = 0.0;
      end = 0.0;
    }
  }

  // For equi-depth, all buckets will have same (approx) number of rows
  long numRowsPerBucket;

  // An array of buckets arranged in increasing order of their start boundaries
  private Bucket[] buckets;

  // whether histogram was already created
  private boolean completed;

  public NumericEquiDepthHistogram() {
    buckets = new Bucket[NUM_BUCKETS];
    for (int i = 0; i < buckets.length; i++) {
      buckets[i] = new Bucket();
    }
    completed = false;
  }

  public void setNumRowsPerBucket(long numRows) {
    this.numRowsPerBucket = numRows;
  }

  @Override
  public Double estimatedSelectivity(RexNode filter) {
    if (!completed) {
      return null;
    } else {
      return 1.0;
    }
  }

  /**
   * Utility method to build a Numeric Equi-Depth Histogram from a t-digest byte array
   * @param tdigest_array
   * @return An instance of NumericEquiDepthHistogram
   */
  public static NumericEquiDepthHistogram buildFromTDigest(byte[] tdigest_array) {
    TDigest tdigest = TDigest.fromBytes(java.nio.ByteBuffer.wrap(tdigest_array));

    NumericEquiDepthHistogram histogram = new NumericEquiDepthHistogram();

    double q = 1.0/NUM_BUCKETS;
    int i = 0;
    for (; i < NUM_BUCKETS; i++) {
      // get the starting point of the i-th quantile
      double start = tdigest.quantile(q * i);
      histogram.buckets[i].start = start;
      if (i > 0) {
        histogram.buckets[i-1].end = start;
      }
    }
    // set the end point of last bucket
    histogram.buckets[i].end = tdigest.quantile(q * i);

    // each bucket stores approx equal number of rows
    histogram.setNumRowsPerBucket(tdigest.size()/NUM_BUCKETS);

    return histogram;
  }

}
