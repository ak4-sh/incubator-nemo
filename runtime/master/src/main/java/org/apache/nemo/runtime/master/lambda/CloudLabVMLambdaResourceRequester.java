package org.apache.nemo.runtime.master.lambda;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.offloading.common.EventHandler;
import org.apache.nemo.offloading.common.NettyChannelInitializer;
import org.apache.nemo.offloading.common.NettyLambdaInboundHandler;
import org.apache.nemo.offloading.common.OffloadingMasterEvent;
import org.apache.reef.tang.annotations.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * LambdaContainerRequester backed by pre-warmed CloudLab VMWorker processes.
 */
public final class CloudLabVMLambdaResourceRequester implements LambdaContainerRequester {
  private static final Logger LOG = LoggerFactory.getLogger(CloudLabVMLambdaResourceRequester.class.getName());

  private final Bootstrap clientBootstrap;
  private final EventLoopGroup clientWorkerGroup;
  private final ConcurrentMap<Channel, EventHandler<OffloadingMasterEvent>> map;
  private final List<String> vmAddressList;
  private final AtomicInteger nextVMIndex = new AtomicInteger(0);

  @Inject
  private CloudLabVMLambdaResourceRequester(@Parameter(EvalConf.VMAddresses.class) final String vmAddresses) {
    this.clientWorkerGroup = new NioEventLoopGroup(10,
      new DefaultThreadFactory("cloudlab-vm-lambda-ClientWorker"));
    this.clientBootstrap = new Bootstrap();
    this.map = new ConcurrentHashMap<>();
    this.clientBootstrap.group(clientWorkerGroup)
      .channel(NioSocketChannel.class)
      .handler(new NettyChannelInitializer(new NettyLambdaInboundHandler(map)))
      .option(ChannelOption.SO_REUSEADDR, true)
      .option(ChannelOption.SO_KEEPALIVE, true);

    this.vmAddressList = new ArrayList<>();
    if (vmAddresses != null && !vmAddresses.isEmpty()) {
      final String[] lines = vmAddresses.split("\\n");
      for (String line : lines) {
        line = line.trim();
        if (!line.isEmpty()) {
          vmAddressList.add(line.split(",", 2)[0]);
        }
      }
    }

    LOG.info("CloudLabVMLambdaResourceRequester initialized with {} VM addresses: {}",
      vmAddressList.size(), vmAddressList);
  }

  @Override
  public synchronized LambdaActivator createRequest(final String controlAddr,
                                                    final int controlPort,
                                                    final String containerType,
                                                    final int capacity,
                                                    final int slot,
                                                    final int memory) {
    if (vmAddressList.isEmpty()) {
      throw new RuntimeException("No CloudLab VM addresses available for offloading");
    }

    final int index = nextVMIndex.getAndIncrement();
    final String vmAddress = vmAddressList.get(index % vmAddressList.size());
    final String[] parts = vmAddress.split(":");
    if (parts.length != 2) {
      throw new RuntimeException("Invalid CloudLab VM address: " + vmAddress);
    }

    final String host = parts[0];
    final int port = Integer.parseInt(parts[1]);
    final int requestId = index + 1;
    final String executorId = "VM-" + requestId;

    LOG.info("Connecting to pre-warmed CloudLab VM worker {} for requestId: {}/{}",
      vmAddress, requestId, executorId);

    ChannelFuture channelFuture;
    final long waitingTime = 1000;
    while (true) {
      final long st = System.currentTimeMillis();
      channelFuture = clientBootstrap.connect(new InetSocketAddress(host, port));
      channelFuture.awaitUninterruptibly(waitingTime);
      if (channelFuture.isSuccess()) {
        break;
      }

      LOG.warn("Connection to CloudLab VM worker {} failed, waiting...", vmAddress);
      final long elapsedTime = System.currentTimeMillis() - st;
      try {
        Thread.sleep(Math.max(1, waitingTime - elapsedTime));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    }

    final Channel openChannel = channelFuture.channel();
    LOG.info("Open channel for CloudLab VM worker {}: {}", vmAddress, openChannel);

    final byte[] bytes = String.format("{\"address\":\"%s\", \"port\": %d, \"requestId\": %d}",
      controlAddr, controlPort, requestId).getBytes();
    openChannel.writeAndFlush(new OffloadingMasterEvent(
      OffloadingMasterEvent.Type.SEND_ADDRESS, bytes, bytes.length));

    return new LambdaActivator() {
      @Override
      public void activate() {
      }

      @Override
      public int getRequestId() {
        return requestId;
      }

      @Override
      public String getExecutorId() {
        return executorId;
      }
    };
  }
}
