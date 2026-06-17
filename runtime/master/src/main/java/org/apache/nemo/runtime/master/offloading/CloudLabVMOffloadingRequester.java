package org.apache.nemo.runtime.master.offloading;

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
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public final class CloudLabVMOffloadingRequester implements OffloadingRequester {

  private static final Logger LOG = LoggerFactory.getLogger(CloudLabVMOffloadingRequester.class.getName());

  private EventLoopGroup clientWorkerGroup;
  private final ConcurrentMap<Channel, EventHandler<OffloadingMasterEvent>> map;
  private Bootstrap clientBootstrap;
  private final Map<String, String> vmChannelMap = new ConcurrentHashMap<>();
  private final AtomicInteger numVMs = new AtomicInteger(0);
  private final ExecutorService waitingExecutor = Executors.newCachedThreadPool();
  private final AtomicInteger requestIdCounter = new AtomicInteger(0);

  private final List<String> vmAddressList;
  private final AtomicInteger nextVMIndex = new AtomicInteger(0);

  @Inject
  private CloudLabVMOffloadingRequester(@Parameter(EvalConf.VMAddresses.class) final String vmAddresses) {
    this.clientWorkerGroup = new NioEventLoopGroup(10,
      new DefaultThreadFactory("cloudlab-vm" + "-ClientWorker"));
    this.clientBootstrap = new Bootstrap();
    this.map = new ConcurrentHashMap<>();
    this.clientBootstrap.group(clientWorkerGroup)
      .channel(NioSocketChannel.class)
      .handler(new NettyChannelInitializer(new NettyLambdaInboundHandler(map)))
      .option(ChannelOption.SO_REUSEADDR, true)
      .option(ChannelOption.SO_KEEPALIVE, true);

    this.vmAddressList = new ArrayList<>();
    if (vmAddresses != null && !vmAddresses.isEmpty()) {
      final String[] lines = vmAddresses.split("\n");
      for (String line : lines) {
        line = line.trim();
        if (!line.isEmpty()) {
          vmAddressList.add(line);
        }
      }
    }
    LOG.info("CloudLabVMOffloadingRequester initialized with {} VM addresses: {}",
      vmAddressList.size(), vmAddressList);
  }

  @Override
  public void start() {
    LOG.info("CloudLabVMOffloadingRequester started");
  }

  @Override
  public synchronized void createChannelRequest(final String controlAddr,
                                                final int controlPort,
                                                final int requestId,
                                                final String executorId) {
    if (vmAddressList.isEmpty()) {
      LOG.error("No VM addresses available for offloading!");
      return;
    }

    final int index = nextVMIndex.getAndIncrement() % vmAddressList.size();
    final String vmAddress = vmAddressList.get(index);
    final String[] parts = vmAddress.split(":");
    final String host = parts[0];
    final int port = Integer.parseInt(parts[1]);

    LOG.info("Connecting to pre-warmed VM worker {} (index={}) for request {}",
      vmAddress, index, requestId);

    waitingExecutor.execute(() -> {
      ChannelFuture channelFuture;
      final long waitingTime = 1000;
      while (true) {
        final long st = System.currentTimeMillis();
        channelFuture = clientBootstrap.connect(new InetSocketAddress(host, port));
        channelFuture.awaitUninterruptibly(waitingTime);
        if (!channelFuture.isSuccess()) {
          LOG.warn("Connection to {} failed, waiting...", vmAddress);
          final long elapsedTime = System.currentTimeMillis() - st;
          try {
            Thread.sleep(Math.max(1, waitingTime - elapsedTime));
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
          }
        } else {
          break;
        }
      }

      final Channel openChannel = channelFuture.channel();
      LOG.info("Open channel for VM worker {}: {}", vmAddress, openChannel);

      final byte[] bytes = String.format("{\"address\":\"%s\", \"port\": %d, \"requestId\": %d}",
        controlAddr, controlPort, requestIdCounter.getAndIncrement()).getBytes();
      openChannel.writeAndFlush(new OffloadingMasterEvent(
        OffloadingMasterEvent.Type.SEND_ADDRESS, bytes, bytes.length));

      numVMs.getAndIncrement();
      LOG.info("Added channel for VM worker {}: {}", vmAddress, openChannel.remoteAddress());
    });
  }

  @Override
  public synchronized void destroyChannel(final Channel channel) {
    final String addr = channel.remoteAddress().toString().split(":")[0];
    vmChannelMap.remove(addr);
    numVMs.getAndDecrement();
    LOG.info("Removed channel for VM worker at {}", addr);
  }

  @Override
  public void destroy() {
    LOG.info("Destroying CloudLabVMOffloadingRequester ({} active channels)", numVMs.get());
  }

  @Override
  public void close() {
    LOG.info("Closing CloudLabVMOffloadingRequester");
  }
}
