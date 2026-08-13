package org.apache.nemo.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.*;
import java.util.Enumeration;

public class NetworkUtils {
  private static final Logger LOG = LoggerFactory.getLogger(NetworkUtils.class.getName());


  public static String getPublicIP() {
    final URL whatismyip;
    try {
      whatismyip = new URL("http://checkip.amazonaws.com");
    } catch (MalformedURLException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    final BufferedReader in;
    try {
      in = new BufferedReader(new InputStreamReader(
        whatismyip.openStream()));
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    String ip = null; //you get the IP as a String
    try {
      ip = in.readLine();
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    return ip;
  }

  public static InetAddress getLocalHostLANAddress() throws UnknownHostException {
    try {
      InetAddress candidateAddress = null;

      final Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
      while (interfaces.hasMoreElements()) {
        final NetworkInterface iface = interfaces.nextElement();
        if (!iface.isUp() || iface.isLoopback() || iface.isVirtual()) {
          continue;
        }

        final Enumeration<InetAddress> addresses = iface.getInetAddresses();
        while (addresses.hasMoreElements()) {
          final InetAddress address = addresses.nextElement();
          if (address.isLoopbackAddress() || !(address instanceof Inet4Address)) {
            continue;
          }

          final String hostAddress = address.getHostAddress();
          if (hostAddress.startsWith("10.10.")) {
            return address;
          }
          if (address.isSiteLocalAddress()) {
            candidateAddress = address;
          } else if (candidateAddress == null) {
            candidateAddress = address;
          }
        }
      }

      if (candidateAddress != null) {
        // We did not find a site-local address, but we found some other non-loopback address.
        // Server might have a non-site-local address assigned to its NIC (or it might be running
        // IPv6 which deprecates the "site-local" concept).
        // Return this non-loopback candidate address...
        return candidateAddress;
      }
      // At this point, we did not find a non-loopback address.
      // Fall back to returning whatever InetAddress.getLocalHost() returns...
      InetAddress jdkSuppliedAddress = InetAddress.getLocalHost();
      if (jdkSuppliedAddress == null) {
        throw new UnknownHostException("The JDK InetAddress.getLocalHost() method unexpectedly returned null.");
      }
      return jdkSuppliedAddress;
    }
    catch (Exception e) {
      UnknownHostException unknownHostException = new UnknownHostException("Failed to determine LAN address: " + e);
      unknownHostException.initCause(e);
      throw unknownHostException;
    }
  }
}
