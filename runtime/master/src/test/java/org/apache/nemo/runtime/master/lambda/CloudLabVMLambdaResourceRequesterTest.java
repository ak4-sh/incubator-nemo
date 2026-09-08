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
package org.apache.nemo.runtime.master.lambda;

import io.netty.channel.Channel;
import org.apache.nemo.offloading.common.OffloadingMasterEvent;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies that a persistent CloudLab VM process receives one fresh handler-invocation request
 * for initial registration and for every later logical worker activation.
 */
public final class CloudLabVMLambdaResourceRequesterTest {
  private Channel requestChannel;

  @Before
  public void setUp() {
    requestChannel = mock(Channel.class);
    when(requestChannel.isOpen()).thenReturn(true);
    when(requestChannel.isActive()).thenReturn(true);
  }

  @Test
  public void testEveryActivationRequestsFreshHandlerInvocation() {
    final CloudLabVMLambdaResourceRequester.CloudLabVMActivator activator = newActivator();

    activator.startInitialInvocation();
    activator.activate();
    activator.activate();

    final ArgumentCaptor<OffloadingMasterEvent> eventCaptor =
      ArgumentCaptor.forClass(OffloadingMasterEvent.class);
    verify(requestChannel, times(3)).writeAndFlush(eventCaptor.capture());

    final List<OffloadingMasterEvent> events = eventCaptor.getAllValues();
    for (OffloadingMasterEvent event : events) {
      assertEquals(OffloadingMasterEvent.Type.SEND_ADDRESS, event.getType());
    }

    assertPayload(events.get(0), 1, "INITIAL");
    assertPayload(events.get(1), 2, "REACTIVATION");
    assertPayload(events.get(2), 3, "REACTIVATION");
    assertEquals(7, activator.getRequestId());
    assertEquals("VM-7", activator.getExecutorId());
  }

  @Test(expected = RuntimeException.class)
  public void testClosedChannelRejectsActivation() {
    when(requestChannel.isOpen()).thenReturn(false);

    newActivator().activate();
  }

  private CloudLabVMLambdaResourceRequester.CloudLabVMActivator newActivator() {
    return new CloudLabVMLambdaResourceRequester.CloudLabVMActivator(
      requestChannel, "10.0.0.1", 19888, 7, "VM-7");
  }

  private void assertPayload(final OffloadingMasterEvent event,
                             final int invocation,
                             final String reason) {
    final String payload = new String(event.getBytes(), StandardCharsets.UTF_8);
    assertTrue(payload.contains("\"address\":\"10.0.0.1\""));
    assertTrue(payload.contains("\"port\": 19888"));
    assertTrue(payload.contains("\"requestId\": 7"));
    assertTrue(payload.contains("\"invocationSequence\": " + invocation));
    assertTrue(payload.contains("\"activationReason\": \"" + reason + "\""));
  }
}
