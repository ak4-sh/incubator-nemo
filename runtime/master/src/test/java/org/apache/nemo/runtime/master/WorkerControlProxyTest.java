/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nemo.runtime.master;

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import org.apache.nemo.offloading.common.OffloadingMasterEvent;
import org.apache.nemo.runtime.master.lambda.LambdaContainerRequester;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Tests the asynchronous worker lifecycle managed by {@link WorkerControlProxy}.
 */
public final class WorkerControlProxyTest {
  private static final int REQUEST_ID = 7;
  private static final String EXECUTOR_ID = "LambdaExecutor7";

  private Channel controlChannel;
  private LambdaContainerRequester.LambdaActivator activator;
  private Set<WorkerControlProxy> pendingActivationWorkers;

  @Before
  public void setUp() {
    controlChannel = mock(Channel.class);
    activator = mock(LambdaContainerRequester.LambdaActivator.class);
    pendingActivationWorkers = new HashSet<>();
  }

  @Test
  public void testNormalActivation() {
    final WorkerControlProxy proxy = newProxy(true);
    deactivateAndAcknowledge(proxy);
    clearInvocations(controlChannel);

    proxy.activate();

    assertTrue(proxy.isActivating());
    assertTrue(pendingActivationWorkers.contains(proxy));
    verify(activator, times(1)).activate();
    verify(controlChannel, never()).writeAndFlush(org.mockito.Matchers.any());
  }

  @Test
  public void testActivationIsDeferredUntilEndAcknowledgement() {
    final WorkerControlProxy proxy = newProxy(true);
    proxy.deactivate();
    clearInvocations(controlChannel);

    proxy.activate();

    assertFalse(proxy.isActive());
    assertFalse(proxy.isActivating());
    assertFalse(proxy.isDeactivated());
    assertTrue(pendingActivationWorkers.isEmpty());
    verify(activator, never()).activate();
    verify(controlChannel, never()).writeAndFlush(org.mockito.Matchers.any());

    proxy.onNext(endAcknowledgement());

    assertTrue(proxy.isActivating());
    assertTrue(pendingActivationWorkers.contains(proxy));
    verify(activator, times(1)).activate();
    verify(controlChannel, never()).writeAndFlush(org.mockito.Matchers.any());
  }

  @Test
  public void testDuplicateDeferredRequestsAreCoalesced() {
    final WorkerControlProxy proxy = newProxy(true);
    proxy.deactivate();

    proxy.activate();
    proxy.activate();
    proxy.onNext(endAcknowledgement());

    assertTrue(proxy.isActivating());
    verify(activator, times(1)).activate();
    assertEquals(1, pendingActivationWorkers.size());
  }

  @Test
  public void testTwoCompleteCyclesUseOneBackendActivationEach() {
    final WorkerControlProxy proxy = newProxy(true);
    deactivateAndAcknowledge(proxy);
    clearInvocations(controlChannel);

    for (int cycle = 1; cycle <= 2; cycle++) {
      proxy.activate();

      assertTrue(proxy.isActivating());
      assertTrue(pendingActivationWorkers.contains(proxy));
      verify(activator, times(cycle)).activate();
      verify(controlChannel, never()).writeAndFlush(org.mockito.Matchers.any());

      proxy.onNext(activationAcknowledgement());
      assertTrue(proxy.isActive());
      assertFalse(proxy.isActivating());
      assertTrue(pendingActivationWorkers.isEmpty());

      proxy.deactivate();
      assertFalse(proxy.isActive());
      proxy.onNext(endAcknowledgement());
      assertTrue(proxy.isDeactivated());
      clearInvocations(controlChannel);
    }
  }

  @Test
  public void testActivationAcknowledgementCompletesDeferredReactivation() {
    final WorkerControlProxy proxy = newProxy(true);
    proxy.deactivate();
    proxy.activate();
    proxy.onNext(endAcknowledgement());

    proxy.onNext(activationAcknowledgement());

    assertTrue(proxy.isActive());
    assertFalse(proxy.isActivating());
    assertTrue(pendingActivationWorkers.isEmpty());
    verify(activator, times(1)).activate();
  }

  @Test
  public void testTaskDeliveryWaitsForDeferredActivationAcknowledgement() {
    final WorkerControlProxy proxy = newProxy(true);
    proxy.deactivate();
    proxy.activate();

    assertTrue(DefaultExecutorRepresenterImpl.shouldWaitForWorkerActivation(proxy));

    proxy.onNext(endAcknowledgement());
    assertTrue(DefaultExecutorRepresenterImpl.shouldWaitForWorkerActivation(proxy));

    proxy.onNext(activationAcknowledgement());
    assertFalse(DefaultExecutorRepresenterImpl.shouldWaitForWorkerActivation(proxy));
  }

  @Test
  public void testDisabledFixRetainsOriginalActivatingTaskBehavior() {
    final WorkerControlProxy proxy = newProxy(false);
    deactivateAndAcknowledge(proxy);
    proxy.activate();

    assertTrue(proxy.isActivating());
    assertFalse(DefaultExecutorRepresenterImpl.shouldWaitForWorkerActivation(proxy));
  }

  @Test
  public void testEndWithoutDeferredRequestLeavesWorkerDeactivated() {
    final WorkerControlProxy proxy = newProxy(true);

    deactivateAndAcknowledge(proxy);

    assertTrue(proxy.isDeactivated());
    assertTrue(pendingActivationWorkers.isEmpty());
    verify(activator, never()).activate();
  }

  @Test(expected = RuntimeException.class)
  public void testDisabledFixRetainsOriginalFailure() {
    final WorkerControlProxy proxy = newProxy(false);
    proxy.deactivate();

    proxy.activate();
  }

  private WorkerControlProxy newProxy(final boolean safeWorkerReactivation) {
    return new WorkerControlProxy(
      REQUEST_ID,
      EXECUTOR_ID,
      controlChannel,
      null,
      activator,
      pendingActivationWorkers,
      safeWorkerReactivation);
  }

  private void deactivateAndAcknowledge(final WorkerControlProxy proxy) {
    proxy.deactivate();
    proxy.onNext(endAcknowledgement());
  }

  private OffloadingMasterEvent endAcknowledgement() {
    return new OffloadingMasterEvent(OffloadingMasterEvent.Type.END, Unpooled.buffer(0));
  }

  private OffloadingMasterEvent activationAcknowledgement() {
    return new OffloadingMasterEvent(OffloadingMasterEvent.Type.ACTIVATE, new byte[0], 0);
  }

}
