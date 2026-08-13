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

import org.apache.nemo.common.RuntimeIdManager;
import org.apache.nemo.common.ir.vertex.executionproperty.ResourcePriorityProperty;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.runtime.master.DefaultExecutorRepresenterImpl;
import org.apache.nemo.runtime.master.ExecutorRepresenter;
import org.apache.nemo.runtime.master.SerializedTaskMap;
import org.apache.nemo.runtime.message.FailedMessageSender;
import org.apache.nemo.runtime.message.MessageEnvironment;
import org.apache.nemo.runtime.message.MessageSender;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.evaluator.*;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.*;

import static org.apache.nemo.runtime.message.MessageEnvironment.ListenerType.EXECUTOR_MESSAGE_LISTENER_ID;

/**
 * (WARNING) This class is not thread-safe.
 * Only a single thread should use the methods of this class.
 * (i.e., runtimeMasterThread in RuntimeMaster)
 *
 * Encapsulates REEF's evaluator management for containers.
 * Serves as a single point of container management in Runtime.
 * We define a unit of resource a container (an evaluator in REEF), and launch a single executor on each container.
 */
// We need an overall cleanup of this class after #60 is resolved.
@DriverSide
@NotThreadSafe
public final class ContainerManager {
  private static final Logger LOG = LoggerFactory.getLogger(ContainerManager.class.getName());

  private boolean isTerminated;

  private final EvaluatorRequestor evaluatorRequestor;
  private final MessageEnvironment messageEnvironment;
  private final ExecutorService serializationExecutorService; // Executor service for scheduling message serialization.

  /**
   * A map containing a latch for the container requests for each resource spec ID.
   */
  private final Map<String, CountDownLatch> requestLatchByResourceSpecId;

  /**
   * Keeps track of evaluator and context requests.
   */
  private final Map<String, ResourceSpecification> pendingContextIdToResourceSpec;
  private final Map<String, String> pendingContextIdToContainerId;
  private final Map<String, List<ResourceSpecification>> pendingContainerRequestsByContainerType;

  /**
   * Remember the resource spec for each evaluator.
   */
  private final Map<AllocatedEvaluator, ResourceSpecification> evaluatorIdToResourceSpec;
  private final Map<String, String> evaluatorIdToExecutorId;

  private final JVMProcessFactory jvmProcessFactory;

  private final SerializedTaskMap serializedTaskMap;

  private final String optPolicy;
  private final String jobId;
  private final ExecutorPlacementPolicy placementPolicy;
  private final String placementReportPath;

  @Inject
  private ContainerManager(@Parameter(JobConf.ScheduleSerThread.class) final int scheduleSerThread,
                           final EvaluatorRequestor evaluatorRequestor,
                           final MessageEnvironment messageEnvironment,
                           final JVMProcessFactory jvmProcessFactory,
                           @Parameter(JobConf.OptimizationPolicy.class) final String optPolicy,
                           @Parameter(JobConf.JobId.class) final String jobId,
                           @Parameter(JobConf.SourceHosts.class) final String sourceHosts,
                           @Parameter(JobConf.ComputeHosts.class) final String computeHosts,
                           @Parameter(JobConf.StrictExecutorPlacement.class) final boolean strictPlacement,
                           @Parameter(JobConf.ExecutorPlacementReportPath.class) final String placementReportPath,
                           final SerializedTaskMap serializedTaskMap) {
    this.isTerminated = false;
    this.serializedTaskMap = serializedTaskMap;
    this.evaluatorRequestor = evaluatorRequestor;
    this.messageEnvironment = messageEnvironment;
    this.pendingContextIdToResourceSpec = new ConcurrentHashMap<>();
    this.pendingContextIdToContainerId = new ConcurrentHashMap<>();
    this.pendingContainerRequestsByContainerType = new ConcurrentHashMap<>();
    this.evaluatorIdToResourceSpec = new ConcurrentHashMap<>();
    this.evaluatorIdToExecutorId = new ConcurrentHashMap<>();
    this.requestLatchByResourceSpecId = new ConcurrentHashMap<>();
    this.serializationExecutorService = Executors.newFixedThreadPool(scheduleSerThread);
    this.jvmProcessFactory = jvmProcessFactory;
    this.optPolicy = optPolicy;
    this.jobId = jobId;
    this.placementPolicy = new ExecutorPlacementPolicy(sourceHosts, computeHosts, strictPlacement);
    this.placementReportPath = placementReportPath == null || placementReportPath.trim().isEmpty()
      ? defaultPlacementReportPath(jobId) : placementReportPath.trim();
    initializePlacementReport();
  }

  public void requestContainer(final int numToRequest,
                               final ResourceSpecification resourceSpecification) {
    requestContainer(numToRequest, resourceSpecification, "Evaluator");
  }

  /**
   * Requests containers/evaluators with the given specifications.
   * @param numToRequest number of containers to request
   * @param resourceSpecification containing the specifications of
   */
  public void requestContainer(final int numToRequest,
                               final ResourceSpecification resourceSpecification,
                               final String runtimeName) {
    if (isTerminated) {
      LOG.info("ContainerManager is terminated, ignoring {}", resourceSpecification.toString());
      return;
    }

    if (numToRequest > 0) {
      // Create a list of executor specifications to be used when containers are allocated.
      final List<String> targetHosts = getTargetHosts(resourceSpecification, numToRequest);
      final List<ResourceSpecification> resourceSpecificationList = new ArrayList<>(numToRequest);
      for (int i = 0; i < numToRequest; i++) {
        final String targetHost = targetHosts.isEmpty() ? "" : targetHosts.get(i);
        resourceSpecificationList.add(resourceSpecification.withPreferredHost(targetHost));
      }

      // Mark the request as pending with the given specifications.
      synchronized (pendingContainerRequestsByContainerType) {
        pendingContainerRequestsByContainerType.putIfAbsent(resourceSpecification.getContainerType(), new ArrayList<>());
        pendingContainerRequestsByContainerType.get(resourceSpecification.getContainerType())
          .addAll(resourceSpecificationList);
      }

      requestLatchByResourceSpecId.put(resourceSpecification.getResourceSpecId(),
        new CountDownLatch(numToRequest));

      final int currContainer = getCurrContainer();

      LOG.info("Request container: {}", resourceSpecification);
      if (targetHosts.isEmpty()) {
        evaluatorRequestor.submit(EvaluatorRequest.newBuilder()
          .setNumber(numToRequest)
          .setMemory(resourceSpecification.getMemory())
          .setNumberOfCores(resourceSpecification.getCapacity())
          .setRuntimeName(runtimeName)
          .build());
      } else {
        for (final String targetHost : targetHosts) {
          evaluatorRequestor.submit(EvaluatorRequest.newBuilder()
            .setNumber(1)
            .setMemory(resourceSpecification.getMemory())
            .setNumberOfCores(resourceSpecification.getCapacity())
            .setRuntimeName(runtimeName)
            .addNodeName(targetHost)
            .build());
        }
      }

      // Wait for request container
      LOG.info("Waiting for container allocation");
      waitForContainer(currContainer + numToRequest);
      LOG.info("End of waiting for container allocation");

    } else {
      LOG.info("Request {} containers", numToRequest);
    }
  }

  public void waitForContainer(int totalContainerNum) {
    while (evaluatorIdToResourceSpec.size() < totalContainerNum) {
      try {
        Thread.sleep(200);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  public int getCurrContainer() {
    return evaluatorIdToResourceSpec.size();
  }

  /**
   * Take the necessary actions in container manager once a container a is allocated.
   * @param executorId of the executor to launch on this container.
   * @param allocatedContainer the allocated container.
   * @param executorConfiguration executor related configuration.
   */
  public void onContainerAllocated(final String executorId,
                                   final AllocatedEvaluator allocatedContainer,
                                   final Configuration executorConfiguration) {
    if (isTerminated) {
      LOG.info("ContainerManager is terminated, closing {}", allocatedContainer.getId());
      allocatedContainer.close();
      return;
    }

    final ResourceSpecification resourceSpecification =
      selectResourceSpecForContainer(executorId, allocatedContainer);

    if (resourceSpecification == null) {
      throw new RuntimeException("We never requested for an extra container " + executorId + "/");
    }

    evaluatorIdToResourceSpec.put(allocatedContainer, resourceSpecification);

    LOG.info("Container type (" + resourceSpecification.getContainerType()
      + ") allocated, will be used for [" + executorId + "]" + ", " +
      allocatedContainer.getEvaluatorDescriptor().getNodeDescriptor().getName());

    pendingContextIdToResourceSpec.put(executorId, resourceSpecification);
    pendingContextIdToContainerId.put(executorId, allocatedContainer.getId());
    evaluatorIdToExecutorId.put(allocatedContainer.getId(), executorId);

    final JVMProcess jvmProcess = jvmProcessFactory.newEvaluatorProcess()
      .addOption("--add-opens=java.base/java.lang=ALL-UNNAMED")
      .addOption("--add-opens=jdk.unsupported/sun.misc=ALL-UNNAMED")
      .addOption("-XX:-OmitStackTraceInFastThrow")
      .addOption("-XX:+PrintGCDetails")
      .addOption("-XX:NewRatio=1")
      .addOption("-XX:InitialHeapSize=" + (resourceSpecification.getMemory() - 100) + "m")
      .addOption("-XX:MaxHeapSize=" + (resourceSpecification.getMemory() - 100) + "m");
      //.addOption("-XX:+UseG1GC")
      //.addOption("-XX:ParallelGCThreads=20")
      //.addOption("-XX:InitiatingHeapOccupancyPercent=70")
      //.addOption("-XX:ConcGCThreads=5")
      //.addOption("-XX:MaxGCPauseMillis=500");
      //.addOption("-verbosegc");

    /*
      .addOption("-Xms" +
        (allocatedContainer.getEvaluatorDescriptor().getMemory() - 200) + "m")
      .addOption("-Xmx" +
        (allocatedContainer.getEvaluatorDescriptor().getMemory() - 200) + "m");
        */

    // .addOption("-verbose:class");
    // LOG.info("Add jvm process for verbose:class");

    // Poison handling
    final Configuration poisonConfiguration = Tang.Factory.getTang().newConfigurationBuilder()
      .bindNamedParameter(JobConf.ExecutorResourceType.class, resourceSpecification.getContainerType())
      .bindNamedParameter(JobConf.ExecutorPosionSec.class, String.valueOf(resourceSpecification.getPoisonSec()))
      .build();

    allocatedContainer.setProcess(jvmProcess);
    allocatedContainer.submitContext(Configurations.merge(executorConfiguration, poisonConfiguration));

  }

  /**
   * Initializes master's connection to the container once launched.
   * A representation of the executor to reside in master is created.
   *
   * @param activeContext for the launched container.
   * @return a representation of the executor. (return an empty Optional if terminated)
   */
  public Optional<ExecutorRepresenter> onContainerLaunched(final ActiveContext activeContext) {
    if (isTerminated) {
      LOG.info("ContainerManager is terminated, closing {}", activeContext.getId());
      activeContext.close();
      return Optional.empty();
    }

    // We set contextId = executorId in NemoDriver when we generate executor configuration.
    final String executorId = activeContext.getId();
    final ResourceSpecification resourceSpec = pendingContextIdToResourceSpec.remove(executorId);
    final String containerId = pendingContextIdToContainerId.remove(executorId);

    // Connect to the executor and initiate Master side's executor representation.
    MessageSender messageSender;
    try {
      messageSender =
          messageEnvironment.asyncConnect(executorId, EXECUTOR_MESSAGE_LISTENER_ID).get();
    } catch (final InterruptedException | ExecutionException e) {
      // TODO #140: Properly classify and handle each RPC failure
      messageSender = new FailedMessageSender();
    }

    // Create the executor representation.
    final ExecutorRepresenter executorRepresenter =
        new DefaultExecutorRepresenterImpl(executorId, resourceSpec, messageSender,
          () -> { activeContext.close(); },
          serializationExecutorService,
            activeContext.getEvaluatorDescriptor().getNodeDescriptor().getName(),
          serializedTaskMap,
          optPolicy);

    recordPlacement(executorId, containerId, activeContext.getEvaluatorDescriptor().getNodeDescriptor().getName(),
      resourceSpec);

    requestLatchByResourceSpecId.get(resourceSpec.getResourceSpecId()).countDown();

    return Optional.of(executorRepresenter);
  }

  /**
   * Re-acquire a new container using the failed container's resource spec.
   * @param failedEvaluatorId of the failed evaluator
   * @return the resource specification of the failed evaluator
   */
  public ResourceSpecification onContainerFailed(final String failedEvaluatorId) {
    ResourceSpecification resourceSpecification = null;
    for (final AllocatedEvaluator evaluator : evaluatorIdToResourceSpec.keySet()) {
      if (evaluator.getId().equals(failedEvaluatorId)) {
        resourceSpecification = evaluatorIdToResourceSpec.remove(evaluator);
      }
    }

    if (resourceSpecification == null) {
      throw new IllegalStateException(failedEvaluatorId + " not in " + evaluatorIdToResourceSpec);
    }
    final String executorId = evaluatorIdToExecutorId.remove(failedEvaluatorId);
    if (executorId != null) {
      pendingContextIdToResourceSpec.remove(executorId);
      pendingContextIdToContainerId.remove(executorId);
    }
    requestContainer(1, resourceSpecification);
    return resourceSpecification;
  }

  public void terminate() {
    if (isTerminated) {
      return;
      // throw new IllegalStateException("Cannot terminate twice");
    }

    evaluatorIdToResourceSpec.keySet().forEach(evalutor -> {
      LOG.info("Terminating evaluator " + evalutor.getId() +
        ", " + evalutor.getEvaluatorDescriptor());
      evalutor.close();
    });
    isTerminated = true;
  }

  /**
   * Selects an executor specification for the executor to be launched on a container.
   * Important! This is a "hack" to get around the inability to mark evaluators with Node Labels in REEF.
   * @return the selected executor specification.
   */
  private ResourceSpecification selectResourceSpecForContainer(
    final String executorId,
    final AllocatedEvaluator allocatedEvaluator) {
    synchronized (pendingContainerRequestsByContainerType) {
      ResourceSpecification selectedResourceSpec = null;
      final String actualHost = allocatedEvaluator.getEvaluatorDescriptor().getNodeDescriptor().getName();

      if (RuntimeIdManager.isLambdaExecutorId(executorId) &&
        pendingContainerRequestsByContainerType.containsKey(ResourcePriorityProperty.LAMBDA)
        && pendingContainerRequestsByContainerType.get(ResourcePriorityProperty.LAMBDA).size() > 0) {
        selectedResourceSpec = pendingContainerRequestsByContainerType.get(ResourcePriorityProperty.LAMBDA)
          .remove(0);
      } else {
        for (final Map.Entry<String, List<ResourceSpecification>> entry
          : pendingContainerRequestsByContainerType.entrySet()) {
          if (!entry.getKey().equals(ResourcePriorityProperty.LAMBDA)) {
            if (entry.getValue().size() > 0) {
              final Iterator<ResourceSpecification> iterator = entry.getValue().iterator();
              while (iterator.hasNext()) {
                final ResourceSpecification spec = iterator.next();
                LOG.info("Entry memory: {}, allocated memory: {}", spec.getMemory(), allocatedEvaluator.getEvaluatorDescriptor().getMemory());
                if (placementPolicy.allows(spec.getContainerType(), spec.getPreferredHost(), actualHost)) {
                  iterator.remove();
                  return spec;
                }
              }
            }
          }
        }
      }

      return selectedResourceSpec;
    }

    // throw new ContainerException(new Throwable("We never requested for an extra container"));
  }

  private String defaultPlacementReportPath(final String configuredJobId) {
    final String safeJobId = configuredJobId == null || configuredJobId.trim().isEmpty()
      ? "unknown" : configuredJobId.trim().replaceAll("[^A-Za-z0-9_.-]", "_");
    return "/tmp/nemo-executor-placement-" + safeJobId + ".csv";
  }

  private void initializePlacementReport() {
    try {
      final Path reportPath = Paths.get(placementReportPath);
      final Path parentPath = reportPath.getParent();
      if (parentPath != null) {
        Files.createDirectories(parentPath);
      }
      try (BufferedWriter writer = Files.newBufferedWriter(reportPath, StandardCharsets.UTF_8,
        StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
        writer.write("timestamp,jobId,executorId,executorType,containerId,requestedHost,physicalHost,placementPassed");
        writer.newLine();
      }
    } catch (final IOException e) {
      LOG.warn("Failed to initialize executor placement report {}", placementReportPath, e);
    }
  }

  private void recordPlacement(final String executorId,
                               final String containerId,
                               final String physicalHost,
                               final ResourceSpecification resourceSpecification) {
    final boolean placementPassed = placementPolicy.isPlacementSatisfied(resourceSpecification.getContainerType(),
      resourceSpecification.getPreferredHost(), physicalHost);
    try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(placementReportPath), StandardCharsets.UTF_8,
      StandardOpenOption.CREATE, StandardOpenOption.APPEND, StandardOpenOption.WRITE)) {
      writer.write(String.format(Locale.US, "%d,%s,%s,%s,%s,%s,%s,%s",
        System.currentTimeMillis(),
        csv(jobId),
        csv(executorId),
        csv(resourceSpecification.getContainerType()),
        csv(containerId),
        csv(resourceSpecification.getPreferredHost()),
        csv(physicalHost),
        placementPassed));
      writer.newLine();
    } catch (final IOException e) {
      LOG.warn("Failed to append executor placement report {}", placementReportPath, e);
    }
  }

  private String csv(final String value) {
    if (value == null) {
      return "";
    }
    return value.replace(",", "_").replace("\n", " ").replace("\r", " ");
  }

  private List<String> getTargetHosts(final ResourceSpecification resourceSpecification,
                                      final int numToRequest) {
    if (resourceSpecification.getPreferredHost() != null && !resourceSpecification.getPreferredHost().isEmpty()) {
      final List<String> targetHosts = new ArrayList<>(numToRequest);
      for (int i = 0; i < numToRequest; i++) {
        targetHosts.add(resourceSpecification.getPreferredHost());
      }
      return targetHosts;
    }
    return placementPolicy.requestHostsFor(resourceSpecification.getContainerType(), numToRequest);
  }
}
