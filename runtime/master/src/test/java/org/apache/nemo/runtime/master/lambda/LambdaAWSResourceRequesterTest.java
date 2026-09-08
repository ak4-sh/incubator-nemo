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

import com.amazonaws.services.lambda.AWSLambdaAsync;
import com.amazonaws.services.lambda.model.InvokeRequest;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/** Ensures backend-neutral worker activation retains the original AWS invocation behavior. */
public final class LambdaAWSResourceRequesterTest {
  @Test
  public void testEveryActivationInvokesAwsLambda() {
    final AWSLambdaAsync awsLambda = mock(AWSLambdaAsync.class);
    final LambdaAWSResourceRequester requester = new LambdaAWSResourceRequester(awsLambda, 1);

    final LambdaContainerRequester.LambdaActivator activator = requester.createRequest(
      "10.0.0.1", 19888, "Lambda", 1, 1, 1769);
    activator.activate();
    activator.activate();

    final ArgumentCaptor<InvokeRequest> requestCaptor = ArgumentCaptor.forClass(InvokeRequest.class);
    verify(awsLambda, times(3)).invokeAsync(requestCaptor.capture());

    final List<InvokeRequest> requests = requestCaptor.getAllValues();
    assertEquals(requests.get(0).getFunctionName(), requests.get(1).getFunctionName());
    assertEquals(requests.get(1).getFunctionName(), requests.get(2).getFunctionName());
    for (InvokeRequest request : requests) {
      final ByteBuffer payloadBuffer = request.getPayload().asReadOnlyBuffer();
      final byte[] payloadBytes = new byte[payloadBuffer.remaining()];
      payloadBuffer.get(payloadBytes);
      final String payload = new String(payloadBytes, StandardCharsets.UTF_8);
      assertTrue(payload.contains("\"requestId\": 0"));
    }
  }
}
