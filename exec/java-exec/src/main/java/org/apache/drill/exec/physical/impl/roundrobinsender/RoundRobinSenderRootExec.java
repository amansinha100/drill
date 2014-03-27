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
package org.apache.drill.exec.physical.impl.roundrobinsender;

import java.util.List;

import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.RoundRobinSender;
import org.apache.drill.exec.physical.impl.RootExec;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.ExecProtos;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.GeneralRPCProtos;
import org.apache.drill.exec.record.FragmentWritableBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.rpc.DrillRpcFuture;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.data.DataTunnel;

/**
 * RoundRobin Sender sends rows to receivers in a round-robin fashion.  Once it has sent to the
 * last receiver, it wraps around to the first receiver.  
 */
public class RoundRobinSenderRootExec implements RootExec {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RoundRobinSenderRootExec.class);
  private FragmentContext context;
  private BroadcastSender operator;
  private OutgoingRecordBatch[] outgoing;

  // private final DataTunnel[] tunnels;
  // private final ExecProtos.FragmentHandle handle;
  private boolean ok;
  private final RecordBatch incoming;
  // private final DrillRpcFuture[] responseFutures;
  private final SendingAccounter sendCount = new SendingAccounter();

  public RoundRobinSenderRootExec(FragmentContext context,
                                  RecordBatch incoming,
                                  RoundRobinSender operator) {
    this.context = context;
    this.incoming = incoming;
    this.operator = operator;

    this.outgoing = new OutgoingRecordBatch[operator.getDestinations().size()];
    int fieldId = 0;
    for (CoordinationProtos.DrillbitEndpoint endpoint : operator.getDestinations()) {
      FragmentHandle opposite = 
        context.getHandle().toBuilder().setMajorFragmentId(operator.getOppositeMajorFragmentId()).setMinorFragmentId(fieldId).build();
      outgoing[fieldId] = new OutgoingRecordBatch(sendCount, operator,
                                                  context.getDataTunnel(endpoint, opposite),
                                                  incoming,
                                                  context,
                                                  fieldId);
      fieldId++;
    }
  }

  @Override
  public boolean next() {

    if (!ok) {
      stop();
      
      return false;
    }

    RecordBatch.IterOutcome out = incoming.next();
    logger.debug("RoundRobin next(): got next record batch with status {}", out);
    switch(out){
      case NONE:
      case STOP:
        try {
          // send any pending batches
          for (OutgoingRecordBatch batch : outgoing) {
            batch.setIsLast();
            batch.flush();
          }
        } catch (SchemaChangeException e) {
          incoming.kill();
          logger.error("Error while creating round-robin sender or flushing outgoing batches", e);
          context.fail(e);
        }
        return false;

      case OK_NEW_SCHEMA:
        try {
          // send all existing batches
          if (partitioner != null) {
            flushOutgoingBatches(false, true);
          }

          // update OutgoingRecordBatch's schema and generate partitioning code
          for (OutgoingRecordBatch b : outgoing) {
            b.initializeBatch();
          }
          createRoundRobinSender();

        } catch (SchemaChangeException e) {
          incoming.kill();
          logger.error("Error while creating round-robin sender or flushing outgoing batches", e);
          context.fail(e);
          return false;
        }
      case OK:
        partitioner.partitionBatch(incoming);
        context.getStats().batchesCompleted.inc(1);
        context.getStats().recordsCompleted.inc(incoming.getRecordCount());
        return true;
      case NOT_YET:
      default:
        throw new IllegalStateException();
    }
  }

  private void createRoundRobinSender() throws SchemaChangeException {

    final ClassGenerator<Partitioner> cg = CodeGenerator.getRoot(Partitioner.TEMPLATE_DEFINITION,
                                                                 context.getFunctionRegistry());


    // generate code to copy from an incoming value vector to the destination partition's outgoing value vector
    JExpression inIndex = JExpr.direct("inIndex");
    JExpression bucket = JExpr.direct("bucket");
    JType outgoingVectorArrayType = cg.getModel().ref(ValueVector.class).array().array();
    JType outgoingBatchArrayType = cg.getModel().ref(OutgoingRecordBatch.class).array();

    // generate evaluate expression to determine the hash
    ClassGenerator.HoldingContainer exprHolder = cg.addExpr(materializedExpr);
    cg.getEvalBlock().decl(JType.parse(cg.getModel(), "int"), "bucket", exprHolder.getValue().mod(JExpr.lit(outgoing.length)));
    cg.getEvalBlock().assign(JExpr.ref("bucket"), cg.getModel().ref(Math.class).staticInvoke("abs").arg(bucket));
    // declare and assign the array of outgoing record batches
    JVar outgoingBatches = cg.clazz.field(JMod.NONE,
        outgoingBatchArrayType,
        "outgoingBatches");
    cg.getSetupBlock().assign(outgoingBatches, JExpr.direct("outgoing"));

    // declare a two-dimensional array of value vectors; batch is first dimension, ValueVector is the second
    JVar outgoingVectors = cg.clazz.field(JMod.NONE,
                                          outgoingVectorArrayType,
                                          "outgoingVectors");

    // create 2d array and build initialization list.  For example:
    //     outgoingVectors = new ValueVector[][] { 
    //                              new ValueVector[] {vv1, vv2},
    //                              new ValueVector[] {vv3, vv4}
    //                       });
    JArray outgoingVectorInit = JExpr.newArray(cg.getModel().ref(ValueVector.class).array());

    int fieldId = 0;
    int batchId = 0;
    for (OutgoingRecordBatch batch : outgoing) {

      JArray outgoingVectorInitBatch = JExpr.newArray(cg.getModel().ref(ValueVector.class));
      for (VectorWrapper<?> vv : batch) {
        // declare outgoing value vector and assign it to the array
        JVar outVV = cg.declareVectorValueSetupAndMember("outgoing[" + batchId + "]",
                                                         new TypedFieldId(vv.getField().getType(),
                                                                          fieldId,
                                                                          false));
        // add vv to initialization list (e.g. { vv1, vv2, vv3 } )
        outgoingVectorInitBatch.add(outVV);
        ++fieldId;
      }

      // add VV array to initialization list (e.g. new ValueVector[] { ... })
      outgoingVectorInit.add(outgoingVectorInitBatch);
      ++batchId;
      fieldId = 0;
    }

    // generate outgoing value vector 2d array initialization list.
    cg.getSetupBlock().assign(outgoingVectors, outgoingVectorInit);

    for (VectorWrapper<?> vvIn : incoming) {
      // declare incoming value vectors
      JVar incomingVV = cg.declareVectorValueSetupAndMember("incoming", new TypedFieldId(vvIn.getField().getType(),
                                                                                         fieldId,
                                                                                         vvIn.isHyper()));

      // generate the copyFrom() invocation with explicit cast to the appropriate type
      Class<?> vvType = TypeHelper.getValueVectorClass(vvIn.getField().getType().getMinorType(),
                                                       vvIn.getField().getType().getMode());
      JClass vvClass = cg.getModel().ref(vvType);
      // the following block generates calls to copyFrom(); e.g.:
      // ((IntVector) outgoingVectors[bucket][0]).copyFrom(inIndex,
      //                                                     outgoingBatches[bucket].getRecordCount(),
      //                                                     vv1);
      cg.getEvalBlock().add(
        ((JExpression) JExpr.cast(vvClass,
              ((JExpression)
                     outgoingVectors
                       .component(bucket))
                       .component(JExpr.lit(fieldId))))
                       .invoke("copyFrom")
                       .arg(inIndex)
                       .arg(((JExpression) outgoingBatches.component(bucket)).invoke("getRecordCount"))
                       .arg(incomingVV));

      ++fieldId;
    }
    // generate the OutgoingRecordBatch helper invocations
    cg.getEvalBlock().add(((JExpression) outgoingBatches.component(bucket)).invoke("incRecordCount"));
    cg.getEvalBlock().add(((JExpression) outgoingBatches.component(bucket)).invoke("flushIfNecessary"));
    try {
      // compile and setup generated code
//      partitioner = context.getImplementationClassMultipleOutput(cg);
      partitioner = context.getImplementationClass(cg);
      partitioner.setup(context, incoming, outgoing);

    } catch (ClassTransformationException | IOException e) {
      throw new SchemaChangeException("Failure while attempting to load generated class", e);
    }
  }


  /**
   * Flush each outgoing record batch, and optionally reset the state of each outgoing record
   * batch (on schema change).  Note that the schema is updated based on incoming at the time
   * this function is invoked.
   *
   * @param isLastBatch    true if this is the last incoming batch
   * @param schemaChanged  true if the schema has changed
   */
  public void flushOutgoingBatches(boolean isLastBatch, boolean schemaChanged) throws SchemaChangeException {
    for (OutgoingRecordBatch batch : outgoing) {
      logger.debug("Attempting to flush all outgoing batches");
      if (isLastBatch)
        batch.setIsLast();
      batch.flush();
      if (schemaChanged) {
        batch.resetBatch();
        batch.initializeBatch();
      }
    }
  }
  
  public void stop() {
    logger.debug("RoundRobin sender stopping.");
    ok = false;
    for(OutgoingRecordBatch b : outgoing){
      b.clear();
    }
    incoming.cleanup();
    sendCount.waitForSendComplete();
  }
}
