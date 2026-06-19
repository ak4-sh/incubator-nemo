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
package org.apache.nemo.driver;

import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.wake.remote.address.LocalAddressProvider;

import javax.inject.Inject;
import java.net.InetAddress;

/**
 * LocalAddressProvider that returns the short hostname (e.g. "node0")
 * instead of the external IP, so that all internal communication
 * uses the CloudLab private network.
 */
public final class ShortHostnameLocalAddressProvider implements LocalAddressProvider {

  private final String cached;

  @Inject
  private ShortHostnameLocalAddressProvider() {
    try {
      final String hostname = InetAddress.getLocalHost().getHostName();
      // Use the short hostname (first part before any dot)
      final int dot = hostname.indexOf('.');
      this.cached = (dot < 0) ? hostname : hostname.substring(0, dot);
    } catch (final Exception e) {
      throw new RuntimeException("Unable to resolve local hostname", e);
    }
  }

  @Override
  public String getLocalAddress() {
    return cached;
  }

  @Override
  public Configuration getConfiguration() {
    return Tang.Factory.getTang().newConfigurationBuilder()
      .bindImplementation(LocalAddressProvider.class, ShortHostnameLocalAddressProvider.class)
      .build();
  }
}
