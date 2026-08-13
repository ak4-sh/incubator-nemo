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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Optional host placement for executor container requests.
 */
public final class ExecutorPlacementPolicy {
  private final List<String> sourceHosts;
  private final List<String> computeHosts;
  private final boolean strict;

  public ExecutorPlacementPolicy(final String sourceHosts,
                                 final String computeHosts,
                                 final boolean strict) {
    this.sourceHosts = parseHostList(sourceHosts);
    this.computeHosts = parseHostList(computeHosts);
    this.strict = strict;
  }

  public static List<String> parseHostList(final String rawHosts) {
    if (rawHosts == null || rawHosts.trim().isEmpty()) {
      return Collections.emptyList();
    }

    final Set<String> deduplicatedHosts = new LinkedHashSet<>();
    for (final String rawHost : rawHosts.split(",")) {
      final String host = rawHost.trim();
      if (!host.isEmpty()) {
        deduplicatedHosts.add(host);
      }
    }
    return Collections.unmodifiableList(new ArrayList<>(deduplicatedHosts));
  }

  public boolean isEnabled() {
    return strict || !sourceHosts.isEmpty() || !computeHosts.isEmpty();
  }

  public boolean isStrict() {
    return strict;
  }

  public List<String> hostsForType(final String containerType) {
    if (ResourcePriorityProperty.SOURCE.equals(containerType)) {
      return sourceHosts;
    }
    if (ResourcePriorityProperty.COMPUTE.equals(containerType)) {
      return computeHosts;
    }
    return Collections.emptyList();
  }

  public List<String> requestHostsFor(final String containerType, final int numToRequest) {
    final List<String> hosts = hostsForType(containerType);
    if (hosts.isEmpty()) {
      if (strict && isPlacedType(containerType) && numToRequest > 0) {
        throw new IllegalArgumentException("Strict executor placement requires hosts for " + containerType);
      }
      return Collections.emptyList();
    }

    if (strict && ResourcePriorityProperty.COMPUTE.equals(containerType) && hosts.size() != numToRequest) {
      throw new IllegalArgumentException("Strict Compute placement requires one host per executor: requested "
        + numToRequest + " executors but got " + hosts.size() + " hosts");
    }
    if (strict && ResourcePriorityProperty.SOURCE.equals(containerType) && numToRequest > hosts.size()) {
      throw new IllegalArgumentException("Strict Source placement requires at least one host per executor: requested "
        + numToRequest + " executors but got " + hosts.size() + " hosts");
    }

    final List<String> requestHosts = new ArrayList<>(numToRequest);
    if (ResourcePriorityProperty.COMPUTE.equals(containerType) && hosts.size() >= numToRequest) {
      requestHosts.addAll(hosts.subList(0, numToRequest));
    } else {
      for (int i = 0; i < numToRequest; i++) {
        requestHosts.add(hosts.get(i % hosts.size()));
      }
    }
    return requestHosts;
  }

  public boolean allows(final String containerType, final String requestedHost, final String actualHost) {
    return !strict || isPlacementSatisfied(containerType, requestedHost, actualHost);
  }

  public boolean isPlacementSatisfied(final String containerType, final String requestedHost, final String actualHost) {
    if (!isPlacedType(containerType)) {
      return true;
    }

    if (requestedHost != null && !requestedHost.isEmpty()) {
      return hostMatches(requestedHost, actualHost);
    }

    final List<String> hosts = hostsForType(containerType);
    return hosts.isEmpty() || hosts.stream().anyMatch(host -> hostMatches(host, actualHost));
  }

  private boolean isPlacedType(final String containerType) {
    return ResourcePriorityProperty.SOURCE.equals(containerType)
      || ResourcePriorityProperty.COMPUTE.equals(containerType);
  }

  private boolean hostMatches(final String expectedHost, final String actualHost) {
    if (expectedHost == null || actualHost == null) {
      return false;
    }
    return expectedHost.equals(actualHost)
      || shortHost(expectedHost).equals(shortHost(actualHost));
  }

  private String shortHost(final String host) {
    final int dotIndex = host.indexOf('.');
    final String noDomain = dotIndex >= 0 ? host.substring(0, dotIndex) : host;
    final int linkIndex = noDomain.indexOf("-link-");
    return linkIndex >= 0 ? noDomain.substring(0, linkIndex) : noDomain;
  }
}
