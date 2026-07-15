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
package org.apache.nemo.runtime.master.resource;

import org.apache.nemo.common.ir.vertex.executionproperty.ResourcePriorityProperty;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests {@link ExecutorPlacementPolicy}.
 */
public final class ExecutorPlacementPolicyTest {
  @Test
  public void testParseHostList() {
    assertEquals(Arrays.asList("node5-link-1", "node9-link-1"),
      ExecutorPlacementPolicy.parseHostList(" node5-link-1, node9-link-1,node5-link-1,, "));
    assertEquals(Collections.emptyList(), ExecutorPlacementPolicy.parseHostList(""));
    assertEquals(Collections.emptyList(), ExecutorPlacementPolicy.parseHostList(null));
  }

  @Test
  public void testMappingExecutorTypesToHosts() {
    final ExecutorPlacementPolicy policy = new ExecutorPlacementPolicy(
      "node5-link-1", "node9-link-1,node10-link-1,node11-link-1,node12-link-1", true);

    assertEquals(Collections.singletonList("node5-link-1"),
      policy.requestHostsFor(ResourcePriorityProperty.SOURCE, 1));
    assertEquals(Arrays.asList("node9-link-1", "node10-link-1", "node11-link-1", "node12-link-1"),
      policy.requestHostsFor(ResourcePriorityProperty.COMPUTE, 4));
    assertEquals(Collections.emptyList(), policy.requestHostsFor(ResourcePriorityProperty.TRANSIENT, 2));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRejectComputeCountMismatchInStrictMode() {
    final ExecutorPlacementPolicy policy = new ExecutorPlacementPolicy(
      "node5-link-1", "node9-link-1,node10-link-1", true);
    policy.requestHostsFor(ResourcePriorityProperty.COMPUTE, 4);
  }

  @Test
  public void testAllowsRequestedHostAndShortHostAliases() {
    final ExecutorPlacementPolicy policy = new ExecutorPlacementPolicy("node5-link-1",
      "node9-link-1,node10-link-1,node11-link-1,node12-link-1", true);

    assertTrue(policy.allows(ResourcePriorityProperty.SOURCE, "node5-link-1", "node5"));
    assertTrue(policy.allows(ResourcePriorityProperty.COMPUTE, "node9-link-1", "node9-link-1"));
    assertFalse(policy.allows(ResourcePriorityProperty.COMPUTE, "node9-link-1", "node5-link-1"));
  }

  @Test
  public void testPreserveUnrestrictedBehavior() {
    final ExecutorPlacementPolicy policy = new ExecutorPlacementPolicy("", "", false);

    assertFalse(policy.isEnabled());
    assertEquals(Collections.emptyList(), policy.requestHostsFor(ResourcePriorityProperty.SOURCE, 1));
    assertTrue(policy.allows(ResourcePriorityProperty.SOURCE, "", "node0-link-1"));
    assertTrue(policy.allows(ResourcePriorityProperty.COMPUTE, "", "node4-link-1"));
  }

  @Test
  public void testNonStrictHostPreferencesDoNotRejectAllocations() {
    final ExecutorPlacementPolicy policy = new ExecutorPlacementPolicy("node5-link-1",
      "node9-link-1,node10-link-1,node11-link-1,node12-link-1", false);

    assertTrue(policy.allows(ResourcePriorityProperty.COMPUTE, "node9-link-1", "node5-link-1"));
    assertFalse(policy.isPlacementSatisfied(ResourcePriorityProperty.COMPUTE, "node9-link-1", "node5-link-1"));
  }
}
