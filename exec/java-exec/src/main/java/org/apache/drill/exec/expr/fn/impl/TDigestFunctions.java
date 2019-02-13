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
package org.apache.drill.exec.expr.fn.impl;

import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.expr.DrillAggFunc;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.NullableBigIntHolder;
import org.apache.drill.exec.expr.holders.NullableIntHolder;
import org.apache.drill.exec.expr.holders.NullableFloat8Holder;
import org.apache.drill.exec.expr.holders.NullableFloat4Holder;
import org.apache.drill.exec.expr.holders.Float8Holder;
import org.apache.drill.exec.expr.holders.Float4Holder;
import org.apache.drill.exec.expr.holders.DateHolder;
import org.apache.drill.exec.expr.holders.NullableDateHolder;
import org.apache.drill.exec.expr.holders.ObjectHolder;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.NullableVarBinaryHolder;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.RepeatedFloat8Vector;

import javax.inject.Inject;

@SuppressWarnings("unused")
public class TDigestFunctions {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TDigestFunctions.class);

  private TDigestFunctions(){}

  @FunctionTemplate(name = "tdigest", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class BigIntTDigestFunction implements DrillAggFunc {
    @Param BigIntHolder in;
    @Workspace ObjectHolder work;
    @Output NullableVarBinaryHolder out;
    @Inject DrillBuf buffer;
    @Inject OptionManager options;
    @Workspace IntHolder compression;

    @Override
    public void setup() {
      work = new ObjectHolder();
      compression.value = (int) options.getLong(org.apache.drill.exec.ExecConstants.TDIGEST_COMPRESSION);
      work.obj = new com.clearspring.analytics.stream.quantile.TDigest(compression.value);
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.quantile.TDigest tdigest = (com.clearspring.analytics.stream.quantile.TDigest) work.obj;
        tdigest.add(in.value);
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.quantile.TDigest tdigest = (com.clearspring.analytics.stream.quantile.TDigest) work.obj;
        try {
          int size = tdigest.smallByteSize();
          java.nio.ByteBuffer byteBuf = java.nio.ByteBuffer.allocate(size);
          tdigest.asSmallBytes(byteBuf);
          out.buffer = buffer.reallocIfNeeded(size);
          out.start = 0;
          out.end = size;
          out.buffer.setBytes(0, byteBuf.array());
          out.isSet = 1;
        } catch (Exception e) {
          throw new org.apache.drill.common.exceptions.DrillRuntimeException("Failed to get TDigest output", e);
        }
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.quantile.TDigest(compression.value);
    }
  }

  @FunctionTemplate(name = "tdigest", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableBigIntTDigestFunction implements DrillAggFunc {
    @Param NullableBigIntHolder in;
    @Workspace ObjectHolder work;
    @Output NullableVarBinaryHolder out;
    @Inject DrillBuf buffer;
    @Inject OptionManager options;
    @Workspace IntHolder compression;

    @Override
    public void setup() {
      work = new ObjectHolder();
      compression.value = (int) options.getLong(org.apache.drill.exec.ExecConstants.TDIGEST_COMPRESSION);
      work.obj = new com.clearspring.analytics.stream.quantile.TDigest(compression.value);
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.quantile.TDigest tdigest = (com.clearspring.analytics.stream.quantile.TDigest) work.obj;
        if (in.isSet == 1) {
          tdigest.add(in.value);
        } else {
          // t-digest does not accept nulls, so use the smallest bigint value
          tdigest.add(Long.MIN_VALUE);
        }
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.quantile.TDigest tdigest = (com.clearspring.analytics.stream.quantile.TDigest) work.obj;
        try {
          int size = tdigest.smallByteSize();
          java.nio.ByteBuffer byteBuf = java.nio.ByteBuffer.allocate(size);
          tdigest.asSmallBytes(byteBuf);
          out.buffer = buffer.reallocIfNeeded(size);
          out.start = 0;
          out.end = size;
          out.buffer.setBytes(0, byteBuf.array());
          out.isSet = 1;
        } catch (Exception e) {
          throw new org.apache.drill.common.exceptions.DrillRuntimeException("Failed to get TDigest output", e);
        }
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.quantile.TDigest(compression.value);
    }
  }

  @FunctionTemplate(name = "tdigest", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class IntTDigestFunction implements DrillAggFunc {
    @Param IntHolder in;
    @Workspace ObjectHolder work;
    @Output NullableVarBinaryHolder out;
    @Inject DrillBuf buffer;
    @Inject OptionManager options;
    @Workspace IntHolder compression;

    @Override
    public void setup() {
      work = new ObjectHolder();
      compression.value = (int) options.getLong(org.apache.drill.exec.ExecConstants.TDIGEST_COMPRESSION);
      work.obj = new com.clearspring.analytics.stream.quantile.TDigest(compression.value);
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.quantile.TDigest tdigest = (com.clearspring.analytics.stream.quantile.TDigest) work.obj;
        tdigest.add(in.value);
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.quantile.TDigest tdigest = (com.clearspring.analytics.stream.quantile.TDigest) work.obj;
        try {
          int size = tdigest.smallByteSize();
          java.nio.ByteBuffer byteBuf = java.nio.ByteBuffer.allocate(size);
          tdigest.asSmallBytes(byteBuf);
          out.buffer = buffer.reallocIfNeeded(size);
          out.start = 0;
          out.end = size;
          out.buffer.setBytes(0, byteBuf.array());
          out.isSet = 1;
        } catch (Exception e) {
          throw new org.apache.drill.common.exceptions.DrillRuntimeException("Failed to get TDigest output", e);
        }
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.quantile.TDigest(compression.value);
    }
  }

  @FunctionTemplate(name = "tdigest", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableIntTDigestFunction implements DrillAggFunc {
    @Param NullableIntHolder in;
    @Workspace ObjectHolder work;
    @Output NullableVarBinaryHolder out;
    @Inject DrillBuf buffer;
    @Inject OptionManager options;
    @Workspace IntHolder compression;

    @Override
    public void setup() {
      work = new ObjectHolder();
      compression.value = (int) options.getLong(org.apache.drill.exec.ExecConstants.TDIGEST_COMPRESSION);
      work.obj = new com.clearspring.analytics.stream.quantile.TDigest(compression.value);
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.quantile.TDigest tdigest = (com.clearspring.analytics.stream.quantile.TDigest) work.obj;
        if (in.isSet == 1) {
          tdigest.add(in.value);
        } else {
          // t-digest does not accept nulls, so use the smallest int value
          tdigest.add(Integer.MIN_VALUE);
        }
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.quantile.TDigest tdigest = (com.clearspring.analytics.stream.quantile.TDigest) work.obj;
        try {
          int size = tdigest.smallByteSize();
          java.nio.ByteBuffer byteBuf = java.nio.ByteBuffer.allocate(size);
          tdigest.asSmallBytes(byteBuf);
          out.buffer = buffer.reallocIfNeeded(size);
          out.start = 0;
          out.end = size;
          out.buffer.setBytes(0, byteBuf.array());
          out.isSet = 1;
        } catch (Exception e) {
          throw new org.apache.drill.common.exceptions.DrillRuntimeException("Failed to get TDigest output", e);
        }
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.quantile.TDigest(compression.value);
    }
  }

  @FunctionTemplate(name = "tdigest", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class Float8TDigestFunction implements DrillAggFunc {
    @Param Float8Holder in;
    @Workspace ObjectHolder work;
    @Output NullableVarBinaryHolder out;
    @Inject DrillBuf buffer;
    @Inject OptionManager options;
    @Workspace IntHolder compression;

    @Override
    public void setup() {
      work = new ObjectHolder();
      compression.value = (int) options.getLong(org.apache.drill.exec.ExecConstants.TDIGEST_COMPRESSION);
      work.obj = new com.clearspring.analytics.stream.quantile.TDigest(compression.value);
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.quantile.TDigest tdigest = (com.clearspring.analytics.stream.quantile.TDigest) work.obj;
        tdigest.add(in.value);
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.quantile.TDigest tdigest = (com.clearspring.analytics.stream.quantile.TDigest) work.obj;
        try {
          int size = tdigest.smallByteSize();
          java.nio.ByteBuffer byteBuf = java.nio.ByteBuffer.allocate(size);
          tdigest.asSmallBytes(byteBuf);
          out.buffer = buffer.reallocIfNeeded(size);
          out.start = 0;
          out.end = size;
          out.buffer.setBytes(0, byteBuf.array());
          out.isSet = 1;
        } catch (Exception e) {
          throw new org.apache.drill.common.exceptions.DrillRuntimeException("Failed to get TDigest output", e);
        }
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.quantile.TDigest(compression.value);
    }
  }

  @FunctionTemplate(name = "tdigest", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableFloat8TDigestFunction implements DrillAggFunc {
    @Param NullableFloat8Holder in;
    @Workspace ObjectHolder work;
    @Output NullableVarBinaryHolder out;
    @Inject DrillBuf buffer;
    @Inject OptionManager options;
    @Workspace IntHolder compression;

    @Override
    public void setup() {
      work = new ObjectHolder();
      compression.value = (int) options.getLong(org.apache.drill.exec.ExecConstants.TDIGEST_COMPRESSION);
      work.obj = new com.clearspring.analytics.stream.quantile.TDigest(compression.value);
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.quantile.TDigest tdigest = (com.clearspring.analytics.stream.quantile.TDigest) work.obj;
        if (in.isSet == 1) {
          tdigest.add(in.value);
        } else {
          // t-digest does not accept nulls, so use the smallest float8 value
          tdigest.add(Double.MIN_VALUE);
        }
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.quantile.TDigest tdigest = (com.clearspring.analytics.stream.quantile.TDigest) work.obj;
        try {
          int size = tdigest.smallByteSize();
          java.nio.ByteBuffer byteBuf = java.nio.ByteBuffer.allocate(size);
          tdigest.asSmallBytes(byteBuf);
          out.buffer = buffer.reallocIfNeeded(size);
          out.start = 0;
          out.end = size;
          out.buffer.setBytes(0, byteBuf.array());
          out.isSet = 1;
        } catch (Exception e) {
          throw new org.apache.drill.common.exceptions.DrillRuntimeException("Failed to get TDigest output", e);
        }
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.quantile.TDigest(compression.value);
    }
  }

  @FunctionTemplate(name = "tdigest", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class Float4TDigestFunction implements DrillAggFunc {
    @Param Float4Holder in;
    @Workspace ObjectHolder work;
    @Output NullableVarBinaryHolder out;
    @Inject DrillBuf buffer;
    @Inject OptionManager options;
    @Workspace IntHolder compression;

    @Override
    public void setup() {
      work = new ObjectHolder();
      compression.value = (int) options.getLong(org.apache.drill.exec.ExecConstants.TDIGEST_COMPRESSION);
      work.obj = new com.clearspring.analytics.stream.quantile.TDigest(compression.value);
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.quantile.TDigest tdigest = (com.clearspring.analytics.stream.quantile.TDigest) work.obj;
        tdigest.add(in.value);
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.quantile.TDigest tdigest = (com.clearspring.analytics.stream.quantile.TDigest) work.obj;
        try {
          int size = tdigest.smallByteSize();
          java.nio.ByteBuffer byteBuf = java.nio.ByteBuffer.allocate(size);
          tdigest.asSmallBytes(byteBuf);
          out.buffer = buffer.reallocIfNeeded(size);
          out.start = 0;
          out.end = size;
          out.buffer.setBytes(0, byteBuf.array());
          out.isSet = 1;
        } catch (Exception e) {
          throw new org.apache.drill.common.exceptions.DrillRuntimeException("Failed to get TDigest output", e);
        }
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.quantile.TDigest(compression.value);
    }
  }

  @FunctionTemplate(name = "tdigest", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableFloat4TDigestFunction implements DrillAggFunc {
    @Param NullableFloat4Holder in;
    @Workspace ObjectHolder work;
    @Output NullableVarBinaryHolder out;
    @Inject DrillBuf buffer;
    @Inject OptionManager options;
    @Workspace IntHolder compression;

    @Override
    public void setup() {
      work = new ObjectHolder();
      compression.value = (int) options.getLong(org.apache.drill.exec.ExecConstants.TDIGEST_COMPRESSION);
      work.obj = new com.clearspring.analytics.stream.quantile.TDigest(compression.value);
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.quantile.TDigest tdigest = (com.clearspring.analytics.stream.quantile.TDigest) work.obj;
        if (in.isSet == 1) {
          tdigest.add(in.value);
        } else {
          // t-digest does not accept nulls, so use the smallest float4 value
          tdigest.add(Float.MIN_VALUE);
        }
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.quantile.TDigest tdigest = (com.clearspring.analytics.stream.quantile.TDigest) work.obj;
        try {
          int size = tdigest.smallByteSize();
          java.nio.ByteBuffer byteBuf = java.nio.ByteBuffer.allocate(size);
          tdigest.asSmallBytes(byteBuf);
          out.buffer = buffer.reallocIfNeeded(size);
          out.start = 0;
          out.end = size;
          out.buffer.setBytes(0, byteBuf.array());
          out.isSet = 1;
        } catch (Exception e) {
          throw new org.apache.drill.common.exceptions.DrillRuntimeException("Failed to get TDigest output", e);
        }
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.quantile.TDigest(compression.value);
    }
  }

  @FunctionTemplate(name = "tdigest", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class DateTDigestFunction implements DrillAggFunc {
    @Param DateHolder in;
    @Workspace ObjectHolder work;
    @Output NullableVarBinaryHolder out;
    @Inject DrillBuf buffer;
    @Inject OptionManager options;
    @Workspace IntHolder compression;

    @Override
    public void setup() {
      work = new ObjectHolder();
      compression.value = (int) options.getLong(org.apache.drill.exec.ExecConstants.TDIGEST_COMPRESSION);
      work.obj = new com.clearspring.analytics.stream.quantile.TDigest(compression.value);
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.quantile.TDigest tdigest = (com.clearspring.analytics.stream.quantile.TDigest) work.obj;
        tdigest.add(in.value);
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.quantile.TDigest tdigest = (com.clearspring.analytics.stream.quantile.TDigest) work.obj;
        try {
          int size = tdigest.smallByteSize();
          java.nio.ByteBuffer byteBuf = java.nio.ByteBuffer.allocate(size);
          tdigest.asSmallBytes(byteBuf);
          out.buffer = buffer.reallocIfNeeded(size);
          out.start = 0;
          out.end = size;
          out.buffer.setBytes(0, byteBuf.array());
          out.isSet = 1;
        } catch (Exception e) {
          throw new org.apache.drill.common.exceptions.DrillRuntimeException("Failed to get TDigest output", e);
        }
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.quantile.TDigest(compression.value);
    }
  }

  @FunctionTemplate(name = "tdigest", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableDateTDigestFunction implements DrillAggFunc {
    @Param NullableDateHolder in;
    @Workspace ObjectHolder work;
    @Output NullableVarBinaryHolder out;
    @Inject DrillBuf buffer;
    @Inject OptionManager options;
    @Workspace IntHolder compression;

    @Override
    public void setup() {
      work = new ObjectHolder();
      compression.value = (int) options.getLong(org.apache.drill.exec.ExecConstants.TDIGEST_COMPRESSION);
      work.obj = new com.clearspring.analytics.stream.quantile.TDigest(compression.value);
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.quantile.TDigest tdigest = (com.clearspring.analytics.stream.quantile.TDigest) work.obj;
        if (in.isSet == 1) {
          tdigest.add(in.value);
        } else {
          // t-digest does not accept nulls, so use the smallest int value
          tdigest.add(Integer.MIN_VALUE);
        }
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.quantile.TDigest tdigest = (com.clearspring.analytics.stream.quantile.TDigest) work.obj;
        try {
          int size = tdigest.smallByteSize();
          java.nio.ByteBuffer byteBuf = java.nio.ByteBuffer.allocate(size);
          tdigest.asSmallBytes(byteBuf);
          out.buffer = buffer.reallocIfNeeded(size);
          out.start = 0;
          out.end = size;
          out.buffer.setBytes(0, byteBuf.array());
          out.isSet = 1;
        } catch (Exception e) {
          throw new org.apache.drill.common.exceptions.DrillRuntimeException("Failed to get TDigest output", e);
        }
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.quantile.TDigest(compression.value);
    }
  }

  @FunctionTemplate(name = "tdigest_merge", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class TDigestMergeFunction implements DrillAggFunc {
    @Param NullableVarBinaryHolder in;
    @Workspace ObjectHolder work;
    @Output NullableVarBinaryHolder out;
    @Inject DrillBuf buffer;
    @Inject OptionManager options;
    @Workspace IntHolder compression;

    @Override
    public void setup() {
      work = new ObjectHolder();
      compression.value = (int) options.getLong(org.apache.drill.exec.ExecConstants.TDIGEST_COMPRESSION);
      work.obj = new com.clearspring.analytics.stream.quantile.TDigest(compression.value);
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.quantile.TDigest tdigest = (com.clearspring.analytics.stream.quantile.TDigest) work.obj;
        try {
          if (in.isSet != 0) {
            byte[] buf = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(in.start, in.end, in.buffer).getBytes();
            com.clearspring.analytics.stream.quantile.TDigest other =
              com.clearspring.analytics.stream.quantile.TDigest.fromBytes(java.nio.ByteBuffer.wrap(buf));
            tdigest.add(other);
          }
        } catch (Exception e) {
          throw new org.apache.drill.common.exceptions.DrillRuntimeException("Failed to merge TDigest output", e);
        }
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.quantile.TDigest tdigest = (com.clearspring.analytics.stream.quantile.TDigest) work.obj;
        try {
          int size = tdigest.smallByteSize();
          java.nio.ByteBuffer byteBuf = java.nio.ByteBuffer.allocate(size);
          tdigest.asSmallBytes(byteBuf);
          out.buffer = buffer.reallocIfNeeded(size);
          out.start = 0;
          out.end = size;
          out.buffer.setBytes(0, byteBuf.array());
          out.isSet = 1;
        } catch (Exception e) {
          throw new org.apache.drill.common.exceptions.DrillRuntimeException("Failed to get TDigest output", e);
        }
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.quantile.TDigest(compression.value);
    }
  }

  @FunctionTemplate(name = "tdigest_decode", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class TDigestDecode implements DrillSimpleFunc {

    @Param
    NullableVarBinaryHolder in;

    @Param
    IntHolder numBuckets;

    @Output
    RepeatedFloat8Vector out;

    @Override
    public void setup() {
    }

    public void eval() {
      // allocate the output vector
      AllocationHelper.allocateNew(out, numBuckets.value);

      if (in.isSet != 0) {
        byte[] din = new byte[in.end - in.start];
        in.buffer.getBytes(in.start, din);
        try {
          // create a TDigest from the incoming bytes
          com.clearspring.analytics.stream.quantile.TDigest final_tdigest =
            com.clearspring.analytics.stream.quantile.TDigest.fromBytes(java.nio.ByteBuffer.wrap(din));
          double q = 1.0/numBuckets.value;
          for (int i = 0; i < numBuckets.value; i++) {
            final_tdigest.quantile(q * i);
          }
        } catch (Exception e) {
          throw new org.apache.drill.common.exceptions.DrillRuntimeException("Failure evaluating tdigest_decode", e);
        }
      }
    }
  }

}
