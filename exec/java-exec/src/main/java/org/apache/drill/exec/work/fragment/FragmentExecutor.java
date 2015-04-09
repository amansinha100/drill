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
package org.apache.drill.exec.work.fragment;

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.drill.common.DeferredException;
import org.apache.drill.exec.coord.ClusterCoordinator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.base.FragmentRoot;
import org.apache.drill.exec.physical.impl.ImplCreator;
import org.apache.drill.exec.physical.impl.RootExec;
import org.apache.drill.exec.proto.BitControl.FragmentStatus;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.UserBitShared.FragmentState;
import org.apache.drill.exec.proto.helper.QueryIdHelper;
import org.apache.drill.exec.work.foreman.DrillbitStatusListener;

/**
 * Responsible for running a single fragment on a single Drillbit. Listens/responds to status request
 * and cancellation messages.
 */
public class FragmentExecutor implements Runnable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FragmentExecutor.class);

  // TODO:  REVIEW:  Can't this be AtomicReference<FragmentState> (so that
  // debugging and logging don't show just integer values--and for type safety)?
  private final AtomicInteger state = new AtomicInteger(FragmentState.AWAITING_ALLOCATION_VALUE);
  private final FragmentRoot rootOperator;
  private final FragmentContext fragmentContext;
  private final StatusReporter listener;
  private volatile boolean canceled;
  private volatile boolean operatorTreeStopped;
  private RootExec root;


  public FragmentExecutor(final FragmentContext context, final FragmentRoot rootOperator,
                          final StatusReporter listener) {
    this.fragmentContext = context;
    this.rootOperator = rootOperator;
    this.listener = listener;
  }

  @Override
  public String toString() {
    return
        super.toString()
        + "[operatorTreeStopped = " + operatorTreeStopped
        + ", state = " + state
        + ", rootOperator = " + rootOperator
        + ", fragmentContext = " + fragmentContext
        + ", listener = " + listener
        + "]";
  }

  public FragmentStatus getStatus() {
    /*
     * If the query is not in a running state, the operator tree is still being constructed and
     * there is no reason to poll for intermediate results.
     *
     * Previously the call to get the operator stats with the AbstractStatusReporter was happening
     * before this check. This caused a concurrent modification exception as the list of operator
     * stats is iterated over while collecting info, and added to while building the operator tree.
     */
    if(state.get() != FragmentState.RUNNING_VALUE) {
      return null;
    }
    final FragmentStatus status =
        AbstractStatusReporter.getBuilder(fragmentContext, FragmentState.RUNNING, null, null).build();
    return status;
  }

  public void cancel() {
    /*
     * Note that this can be called from threads *other* than the one running this runnable(), so
     * we need to be careful about the state transitions that can result. We set the canceled flag,
     * and this is checked in the run() loop, where action will be taken as soon as possible.
     *
     * If the run loop has already exited, because we've already either completed or failed the query,
     * then the request to cancel is a no-op anyway, so it doesn't matter that we won't see the flag.
     */
    canceled = true;

    // this needs to be done here so this is what operators monitor to know they should terminate early.
    fragmentContext.cancel();
  }

  public void receivingFragmentFinished(FragmentHandle handle) {
    cancel();
    if (root != null) {
      root.receivingFragmentFinished(handle);
    }
  }

  @Override
  public void run() {
    final Thread myThread = Thread.currentThread();
    final String originalThreadName = myThread.getName();
    final FragmentHandle fragmentHandle = fragmentContext.getHandle();
    final ClusterCoordinator clusterCoordinator = fragmentContext.getDrillbitContext().getClusterCoordinator();
    final DrillbitStatusListener drillbitStatusListener = new FragmentDrillbitStatusListener();

    try {
      final String newThreadName = String.format("%s:frag:%s:%s",
          QueryIdHelper.getQueryId(fragmentHandle.getQueryId()),
          fragmentHandle.getMajorFragmentId(), fragmentHandle.getMinorFragmentId());
      myThread.setName(newThreadName);

      root = ImplCreator.getExec(fragmentContext, rootOperator);
      clusterCoordinator.addDrillbitStatusListener(drillbitStatusListener);

      logger.debug("Starting fragment runner. {}:{}",
          fragmentHandle.getMajorFragmentId(), fragmentHandle.getMinorFragmentId());
      if (!updateStateOrFail(FragmentState.AWAITING_ALLOCATION, FragmentState.RUNNING)) {
        logger.warn("Unable to set fragment state to RUNNING.  Cancelled or failed?");
        return;
      }

      /*
       * Run the query until root.next returns false OR cancel() changes the
       * state.
       * Note that
       */
      try{
        while (state.get() == FragmentState.RUNNING_VALUE && !canceled && root.next()) {
          // loop
        }
      } catch (AssertionError | Exception e) {
        fragmentContext.fail(e);
      }

      /*
       * We closeOutResources() here as we're done. That's because this can also throw exceptions that we also want to
       * treat as failures of the request, even if the request did fine up until this point. Any failures there will be
       * caught in the catch clause below, which will be reported to the user. If they were to come from the finally
       * clause, the we will simply log the failure.
       */
      closeOutResources(false);


      /*
       * If we've gotten this far, we're either successfully completed the fragment or successfully canceled it. Report
       * that status back to foreman.
       */
      final FragmentState finalState = canceled ? FragmentState.CANCELLED : FragmentState.FINISHED;
      updateStateOrFail(FragmentState.RUNNING, finalState);

    } catch (AssertionError | Exception e) {
      internalFail(e);
    } finally {
      clusterCoordinator.removeDrillbitStatusListener(drillbitStatusListener);

      // Final check to make sure RecordBatches are cleaned up.
      try {
        closeOutResources(true);
      } catch (Exception e) {
        // We shouldn't leak any exceptions here. If we did, they would leak to the JVM. Since we've already completed
        // our messaging to the foreman, the only thing we can do here is log the failure.
        logger.error("Failue in final cleanup of fragment {}", QueryIdHelper.getQueryIdentifier(this.getContext().getHandle()), e);
      }

      myThread.setName(originalThreadName);
    }
  }

  private static final String CLOSE_FAILURE = "Failure while closing out resources";

  private void closeOutResources(final boolean isFinalClose) throws Exception {

    /*
     * The operator tree can only be closed once.  As such, Because of the way this method can be called, it needs to be idempotent; it must
     * be safe to call it more than once. We use this flag to bypass the body if it has
     * been called before.
     */
    synchronized(this) { // synchronize for the state of closed
      if (!operatorTreeStopped) {
        try {
          root.stop(); // TODO make this an AutoCloseable so we can detect lack of closure
        } catch (Exception e) {
          logger.warn(CLOSE_FAILURE, e);
          fragmentContext.getDeferredException().addException(e);
        }
        operatorTreeStopped = true;
      }
    }


    if(!isFinalClose){
      fragmentContext.preClose();
    }else{
      fragmentContext.close();
    }

  }

  private void internalFail(final Throwable excep) {
    state.set(FragmentState.FAILED_VALUE);
    listener.fail(fragmentContext.getHandle(), "Failure while running fragment.", excep);
  }

  /**
   * Updates the fragment state with the given state
   *
   * @param  to  target state
   */
  private void updateState(final FragmentState to) {
    state.set(to.getNumber());
    listener.stateChanged(fragmentContext.getHandle(), to);
  }

  /**
   * Updates the fragment state only iff the current state matches the expected.
   *
   * @param  expected  expected current state
   * @param  to  target state
   * @return true only if update succeeds
   */
  private boolean checkAndUpdateState(final FragmentState expected, final FragmentState to) {
    final boolean success = state.compareAndSet(expected.getNumber(), to.getNumber());
    if (success) {
      listener.stateChanged(fragmentContext.getHandle(), to);
    } else {
      logger.debug("State change failed. Expected state: {} -- target state: {} -- current state: {}.",
          expected.name(), to.name(), FragmentState.valueOf(state.get()));
    }
    return success;
  }

  /**
   * Returns true if the fragment is in a terminal state
   */
  private boolean isCompleted() {
    return state.get() == FragmentState.CANCELLED_VALUE
        || state.get() == FragmentState.FAILED_VALUE
        || state.get() == FragmentState.FINISHED_VALUE;
  }

  /**
   * Update the state if current state matches expected or fail the fragment if state transition fails even though
   * fragment is not in a terminal state.
   *
   * @param expected  current expected state
   * @param to  target state
   * @return true only if update succeeds
   */
  private boolean updateStateOrFail(final FragmentState expected, final FragmentState to) {
    final boolean updated = checkAndUpdateState(expected, to);
    if (!updated && !isCompleted()) {
      final String msg = "State was different than expected while attempting to update state from %s to %s"
          + "however current state was %s.";
      internalFail(new StateTransitionException(
          String.format(msg, expected.name(), to.name(), FragmentState.valueOf(state.get()))));
    }
    return updated;
  }

  public FragmentContext getContext() {
    return fragmentContext;
  }

  private class FragmentDrillbitStatusListener implements DrillbitStatusListener {
    @Override
    public void drillbitRegistered(final Set<CoordinationProtos.DrillbitEndpoint> registeredDrillbits) {
    }

    @Override
    public void drillbitUnregistered(final Set<CoordinationProtos.DrillbitEndpoint> unregisteredDrillbits) {
      // if the defunct Drillbit was running our Foreman, then cancel the query
      final DrillbitEndpoint foremanEndpoint = FragmentExecutor.this.fragmentContext.getForemanEndpoint();
      if (unregisteredDrillbits.contains(foremanEndpoint)) {
        logger.warn("Foreman {} no longer active.  Cancelling fragment {}.",
                    foremanEndpoint.getAddress(),
                    QueryIdHelper.getQueryIdentifier(fragmentContext.getHandle()));
        FragmentExecutor.this.cancel();
      }
    }
  }
}
