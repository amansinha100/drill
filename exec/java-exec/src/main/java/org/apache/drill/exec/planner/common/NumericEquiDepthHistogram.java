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


import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.List;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexLiteral;
import com.clearspring.analytics.stream.quantile.TDigest;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;

/**
 * A column specific equi-depth histogram which is meant for numeric data types
 */
@JsonTypeName("numeric-equi-depth")
public class NumericEquiDepthHistogram implements Histogram {

  /**
   * Use a small non-zero selectivity rather than 0 to account for the fact that
   * histogram boundaries are approximate and even if some values lie outside the
   * range, we cannot be absolutely sure
   */
  static final double SMALL_SELECTIVITY = 0.0001;

  // For equi-depth, all buckets will have same (approx) number of rows
  @JsonProperty("numRowsPerBucket")
  private double numRowsPerBucket;

  // An array of buckets arranged in increasing order of their start boundaries
  // Note that the buckets only maintain the start point of the bucket range.
  // End point is assumed to be the same as the start point of next bucket, although
  // when evaluating the filter selectivity we should treat the interval as [start, end)
  // i.e closed on the start and open on the end
  @JsonProperty("buckets")
  private Double[] buckets;

  /**
   * Helper class to maintain start bucket and end bucket information for a single range predicate
   */
  private class BucketRange {
    private int startBucketId;   // 0-based index of the starting bucket
    private int endBucketId;     // 0-based index of the ending bucket
    private double startBucketFraction; // fraction of rows in start bucket that qualify the predicate
    private double endBucketFraction;   // fraction of rows in end bucket that qualify the predicate
    private boolean isEmpty;  // shortcut for an empty range

    protected BucketRange(boolean isEmpty) {
      this(-1, Integer.MAX_VALUE, 1.0, 1.0, isEmpty);
    }

    protected BucketRange(int start, int end, double startFraction, double endFraction) {
      this(start, end, startFraction, endFraction, false);
    }

    protected BucketRange(int start, int end, double startFraction, double endFraction, boolean isEmpty) {
      this.startBucketId = start;
      this.endBucketId = end;
      this.startBucketFraction = startFraction;
      this.endBucketFraction = endFraction;
      this.isEmpty = isEmpty;
    }

    // intersect this bucket range with the other and store the result in this bucket range
    protected void intersect(BucketRange other) {
      if (this.isEmpty) {
        return;
      }
      if (other.isEmpty) {
        this.isEmpty = true;
      } else {
        if (other.startBucketId > this.startBucketId ||
          (other.startBucketId == this.startBucketId && other.startBucketFraction < this.startBucketFraction)) {
          this.startBucketId = other.startBucketId;
          this.startBucketFraction = other.startBucketFraction;
        }
        if (other.endBucketId < this.endBucketId ||
          (other.endBucketId == this.endBucketId && other.endBucketFraction < this.endBucketFraction)) {
          this.endBucketId = other.endBucketId;
          this.endBucketFraction = other.endBucketFraction;
        }
      }
    }

    protected void setIsEmpty(boolean isEmpty) {
      this.isEmpty = isEmpty;
    }

    protected long getRowCount() {
      if (isEmpty) {
        return 0;
      }

      Preconditions.checkArgument(startBucketId <= endBucketId);

      long numSelectedRows;
      final int last = buckets.length - 1;
      // if the endBucketId corresponds to the last endpoint, then adjust it to be one less
      if (endBucketId == last) {
        endBucketId = last - 1;
      }
      if (startBucketId == endBucketId) {
        numSelectedRows = (long) (startBucketFraction * numRowsPerBucket);
      } else {
        numSelectedRows = (long) ((startBucketFraction + endBucketFraction) * numRowsPerBucket + (endBucketId - startBucketId - 1) * numRowsPerBucket);
      }

      return numSelectedRows;
    }
  }

  // Default constructor for deserializer
  public NumericEquiDepthHistogram() {}

  public NumericEquiDepthHistogram(int numBuckets) {
    // If numBuckets = N, we are keeping N + 1 entries since the (N+1)th bucket's
    // starting value is the MAX value for the column and it becomes the end point of the
    // Nth bucket.
    buckets = new Double[numBuckets + 1];
    for (int i = 0; i < buckets.length; i++) {
      buckets[i] = new Double(0.0);
    }
    numRowsPerBucket = -1;
  }

  public double getNumRowsPerBucket() {
    return numRowsPerBucket;
  }

  public void setNumRowsPerBucket(double numRows) {
    this.numRowsPerBucket = numRows;
  }

  public Double[] getBuckets() {
    return buckets;
  }


  /**
   * Estimate the selectivity of a filter which may contain several range predicates and in the general case is of
   * type: col op value1 AND col op value2 AND col op value3 ...
   *  <p>
   *    e.g a > 10 AND a < 50 AND a >= 20 AND a <= 70 ...
   *  </p>
   * Even though in most cases it will have either 1 or 2 range conditions, we still have to handle the general case
   * For each conjunct, we will find the histogram bucket ranges and intersect them, taking into account that the
   * first and last bucket may be partially covered and all other buckets in the middle are fully covered.
   */
  @Override
  public Double estimatedSelectivity(final RexNode columnFilter, final long totalRowCount) {
    if (numRowsPerBucket == 0) {
      return null;
    }

    // at a minimum, the histogram should have a start and end point of 1 bucket, so at least 2 entries
    Preconditions.checkArgument(buckets.length >= 2,  "Histogram has invalid number of entries");

    List<RexNode> filterList = RelOptUtil.conjunctions(columnFilter);

    final int first = 0;
    final int last = buckets.length - 1;

    BucketRange currentRange = new BucketRange(false);

    for (RexNode filter : filterList) {
      BucketRange tmpRange = getBucketRange(filter);
      if (tmpRange != null) {
        currentRange.intersect(tmpRange);
      }
    }
    long numSelectedRows = currentRange.getRowCount();
    return numSelectedRows == 0 ? SMALL_SELECTIVITY : (double)numSelectedRows/totalRowCount;
  }

  private BucketRange getBucketRange(final RexNode filter) {
    final int first = 0;
    final int last = buckets.length - 1;

    // number of buckets is 1 less than the total # entries in the buckets array since last
    // entry is the end point of the last bucket
    final int numBuckets = buckets.length - 1;
    if (filter instanceof RexCall) {
      // get the operator
      SqlOperator op = ((RexCall) filter).getOperator();
      if (op.getKind() == SqlKind.GREATER_THAN ||
        op.getKind() == SqlKind.GREATER_THAN_OR_EQUAL) {
        Double value = getLiteralValue(filter);
        if (value != null) {

          // *** Handle the boundary conditions first ***

          // if value is less than or equal to the first bucket's start point then all rows qualify
          int result = value.compareTo(buckets[first]);
          if (result <= 0) {
            return new BucketRange(first, last - 1, 1.0, 1.0, false);
          }
          // if value is greater than the end point of the last bucket, then none of the rows qualify
          result = value.compareTo(buckets[last]);
          if (result > 0) {
            return new BucketRange(true);
          } else if (result == 0) {
            if (op.getKind() == SqlKind.GREATER_THAN_OR_EQUAL) {
              // value is exactly equal to the last bucket's end point so we take the ratio 1/bucket_width
              double endBucketFraction = (double) 1.0 / (buckets[last] - buckets[last - 1]);
              return new BucketRange(last - 1, last - 1, endBucketFraction, endBucketFraction);
            } else {
              // predicate is 'column > value' and value is equal to last bucket's endpoint, so none of
              // the rows qualify
              return new BucketRange(true);
            }
          }

          // *** End of boundary conditions ****

          int n = getContainingBucket(value, numBuckets);
          if (n >= 0) {
            double startBucketFraction = ((double)(buckets[n + 1] - value)) / (buckets[n + 1] - buckets[n]);
            return new BucketRange(n, last - 1, startBucketFraction, 1.0);
          } else {
            // value does not exist in any of the buckets
            return new BucketRange(true);
          }
        }
      } else if (op.getKind() == SqlKind.LESS_THAN ||
        op.getKind() == SqlKind.LESS_THAN_OR_EQUAL) {
        Double value = getLiteralValue(filter);
        if (value != null) {

          // *** Handle the boundary conditions first ***

          // if value is greater than the last bucket's end point then all rows qualify
          int result = value.compareTo(buckets[last]);
          if (result >= 0) {
            return new BucketRange(first, last - 1, 1.0, 1.0);
          }
          // if value is less than the first bucket's start point then none of the rows qualify
          result = value.compareTo(buckets[first]);
          if (result < 0) {
            return new BucketRange(true);
          } else if (result == 0) {
            if (op.getKind() == SqlKind.LESS_THAN_OR_EQUAL) {
              // value is exactly equal to the first bucket's start point so we take the ratio 1/bucket_width
              double startBucketFraction = (double) 1.0 / (buckets[first + 1] - buckets[first]);
              return new BucketRange(first, first, startBucketFraction, startBucketFraction);
            } else {
              // predicate is 'column < value' and value is equal to first bucket's start point, so none of
              // the rows qualify
              return new BucketRange(true);
            }
          }

          // *** End of boundary conditions ****

          int n = getContainingBucket(value, numBuckets);
          if (n >= 0) {
            double endBucketFraction = ((double)(value - buckets[n])) / (buckets[n + 1] - buckets[n]);
            return new BucketRange(first, n, 1.0, endBucketFraction);
          } else {
            // value does not exist in any of the buckets
            return new BucketRange(true);
          }
        }
      }
    }
    return null;
  }

  private int getContainingBucket(final Double value, final int numBuckets) {
    int i = 0;
    int containing_bucket = -1;
    // check which bucket this value falls in
    for (; i <= numBuckets; i++) {
      int result = buckets[i].compareTo(value);
      if (result > 0) {
        containing_bucket = i - 1;
        break;
      } else if (result == 0) {
        containing_bucket = i;
        break;
      }
    }
    return containing_bucket;
  }

  private Double getLiteralValue(final RexNode filter) {
    Double value = null;
    List<RexNode> operands = ((RexCall) filter).getOperands();
    if (operands.size() == 2 && operands.get(1) instanceof RexLiteral) {
      RexLiteral l = ((RexLiteral) operands.get(1));

      switch (l.getTypeName()) {
        case DATE:
        case TIMESTAMP:
        case TIME:
          value = (double) ((java.util.Calendar) l.getValue()).getTimeInMillis();
          break;
        case INTEGER:
        case BIGINT:
        case FLOAT:
        case DOUBLE:
        case DECIMAL:
        case BOOLEAN:
          value = l.getValueAs(Double.class);
          break;
        default:
          break;
      }
    }
    return value;
  }

  /**
   * Build a Numeric Equi-Depth Histogram from a t-digest byte array
   * @param tdigest_array
   * @param numBuckets
   * @param nonNullCount
   * @return An instance of NumericEquiDepthHistogram
   */
  public static NumericEquiDepthHistogram buildFromTDigest(final byte[] tdigest_array,
                                                           final int numBuckets,
                                                           final long nonNullCount) {
    TDigest tdigest = TDigest.fromBytes(java.nio.ByteBuffer.wrap(tdigest_array));

    NumericEquiDepthHistogram histogram = new NumericEquiDepthHistogram(numBuckets);

    final double q = 1.0/numBuckets;
    int i = 0;
    for (; i < numBuckets; i++) {
      // get the starting point of the i-th quantile
      double start = tdigest.quantile(q * i);
      histogram.buckets[i] = start;
    }
    // for the N-th bucket, the end point corresponds to the 1.0 quantile but we don't keep the end
    // points; only the start point, so this is stored as the start point of the (N+1)th bucket
    histogram.buckets[i] = tdigest.quantile(1.0);

    // Each bucket stores approx equal number of rows.  Here, we take into consideration the nonNullCount
    // supplied since the stats may have been collected with sampling.  Sampling of 20% means only 20% of the
    // tuples will be stored in the t-digest.  However, the overall stats such as totalRowCount, nonNullCount and
    // NDV would have already been extrapolated up from the sample. So, we take the max of the t-digest size and
    // the supplied nonNullCount. Why non-null ? Because only non-null values are stored in the t-digest.
    double numRowsPerBucket = (double)(Math.max(tdigest.size(), nonNullCount))/numBuckets;
    histogram.setNumRowsPerBucket(numRowsPerBucket);

    return histogram;
  }

}
