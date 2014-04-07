/*******************************************************************************
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
 ******************************************************************************/

package org.apache.drill.exec.planner.cost;

import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptUtil;

public class DrillCostBase implements DrillRelOptCost {

  static final int byteCpuCost = 1; 
  static final int byteNetworkCost = 150 * byteCpuCost;
  static final int byteDiskReadCost = 4 * byteNetworkCost;
  static final int byteDiskWriteCost = 4 * byteNetworkCost;
      
  static final DrillCostBase INFINITY =
      new DrillCostBase(
          Double.POSITIVE_INFINITY,
          Double.POSITIVE_INFINITY,
          Double.POSITIVE_INFINITY, 
          Double.POSITIVE_INFINITY) {
        public String toString() {
          return "{inf}";
        }
      };

  static final DrillCostBase HUGE =
      new DrillCostBase(Double.MAX_VALUE, 
        Double.MAX_VALUE, 
        Double.MAX_VALUE, 
        Double.MAX_VALUE) {
        public String toString() {
          return "{huge}";
        }
      };

  static final DrillCostBase ZERO =
      new DrillCostBase(0.0, 0.0, 0.0, 0.0) {
        public String toString() {
          return "{0}";
        }
      };

  static final DrillCostBase TINY =
      new DrillCostBase(1.0, 1.0, 0.0, 0.0) {
        public String toString() {
          return "{tiny}";
        }
      };

  final double rowCount;
  final double cpu;
  final double io;
  final double network;

  DrillCostBase(double rowCount, double cpu, double io, double network) {
    this.rowCount = rowCount;
    this.cpu = cpu;
    this.io = io;
    this.network = network;
  }

	@Override
	public double getRows() {
		return rowCount;
	}

	@Override
	public double getCpu() {
		return cpu;
	}

	@Override
	public double getIo() {
		return io;
	}

	@Override
	public double getNetwork() {
		return network;
	}

	@Override
	public boolean isInfinite() {
    return (this == INFINITY)
      || (this.cpu == Double.POSITIVE_INFINITY)
      || (this.io == Double.POSITIVE_INFINITY)
      || (this.network == Double.POSITIVE_INFINITY) 
      || (this.rowCount == Double.POSITIVE_INFINITY);
	}

	@Override
	public boolean equals(RelOptCost other) {
    return this == other
      || (other instanceof DrillCostBase
          && (this.cpu == ((DrillCostBase) other).cpu)
          && (this.io == ((DrillCostBase) other).io)
          && (this.network == ((DrillCostBase) other).network)
          && (this.rowCount == ((DrillCostBase) other).rowCount));
	}

	@Override
	public boolean isEqWithEpsilon(RelOptCost other) {
    if (!(other instanceof DrillCostBase)) {
      return false;
    }
    DrillCostBase that = (DrillCostBase) other;
    return (this == that) 
      || ((Math.abs(this.cpu - that.cpu) < RelOptUtil.EPSILON)
          && (Math.abs(this.io - that.io) < RelOptUtil.EPSILON)
          && (Math.abs(this.network - that.network) < RelOptUtil.EPSILON)
          && (Math.abs(this.rowCount - that.rowCount) < RelOptUtil.EPSILON));
	}

	@Override
	public boolean isLe(RelOptCost other) {
    DrillCostBase that = (DrillCostBase) other;
    return (this == that)
        || ((this.cpu <= that.cpu)
        && (this.io <= that.io) 
        && (this.network <= that.network)
        && (this.rowCount <= that.rowCount));
	}

	@Override
	public boolean isLt(RelOptCost other) {
    DrillCostBase that = (DrillCostBase) other;
    return (this == that)
        || ((this.cpu < that.cpu)
        && (this.io < that.io) 
        && (this.network < that.network)
        && (this.rowCount < that.rowCount));
	}

	@Override
	public RelOptCost plus(RelOptCost other) {
    DrillCostBase that = (DrillCostBase) other;
    if ((this == INFINITY) || (that == INFINITY)) {
      return INFINITY;
    }
    return new DrillCostBase(
        this.rowCount + that.rowCount,
        this.cpu + that.cpu,
        this.io + that.io, 
        this.network + that.network);
	}

	@Override
	public RelOptCost minus(RelOptCost other) {
    if (this == INFINITY) {
      return this;
    }
    DrillCostBase that = (DrillCostBase) other;
    return new DrillCostBase(
        this.rowCount - that.rowCount,
        this.cpu - that.cpu,
        this.io - that.io, 
        this.network - that.network);
	}

	@Override
	public RelOptCost multiplyBy(double factor) {
    if (this == INFINITY) {
      return this;
    }
    return new DrillCostBase(rowCount * factor, cpu * factor, io * factor, network * factor);
	}

	@Override
	public double divideBy(RelOptCost cost) {
    // Compute the geometric average of the ratios of all of the factors
    // which are non-zero and finite.
    DrillCostBase that = (DrillCostBase) cost;
    double d = 1;
    double n = 0;
    if ((this.rowCount != 0)
        && !Double.isInfinite(this.rowCount)
        && (that.rowCount != 0)
        && !Double.isInfinite(that.rowCount)) {
      d *= this.rowCount / that.rowCount;
      ++n;
    }
    if ((this.cpu != 0)
        && !Double.isInfinite(this.cpu)
        && (that.cpu != 0)
        && !Double.isInfinite(that.cpu)) {
      d *= this.cpu / that.cpu;
      ++n;
    }
    if ((this.io != 0)
        && !Double.isInfinite(this.io)
        && (that.io != 0)
        && !Double.isInfinite(that.io)) {
      d *= this.io / that.io;
      ++n;
    }
    if ((this.network != 0)
        && !Double.isInfinite(this.network)
        && (that.network != 0)
        && !Double.isInfinite(that.network)) {
      d *= this.network / that.network;
      ++n;
    }

    if (n == 0) {
      return 1.0;
    }
    return Math.pow(d, 1 / n);
	}

}
