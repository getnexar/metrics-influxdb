package metrics_influxdb.measurements;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import metrics_influxdb.api.protocols.UDPInfluxdbProtocol;
import metrics_influxdb.misc.Miscellaneous;
import metrics_influxdb.serialization.line.Inliner;

public class UDPInlinerSender extends QueueableSender {
    private final static Logger LOGGER = LoggerFactory.getLogger(UDPInlinerSender.class);
    private static int MAX_MEASURES_IN_SINGLE_POST = 5000;
    private final Inliner inliner;
	private final InetSocketAddress serverAddress;

    public UDPInlinerSender(UDPInfluxdbProtocol protocol) {
        super(MAX_MEASURES_IN_SINGLE_POST);
        inliner = new Inliner();
        serverAddress = new InetSocketAddress(protocol.getHost(), protocol.getPort());
    }

    @Override
    protected boolean doSend(Collection<Measurement> measures) {
        boolean returnValue = true;
        if (measures.isEmpty()) {
            return true;
        }
        for (Measurement measure: measures) {
          DatagramChannel channel = null;

          String measuresAsString = inliner.inline(measure);
          try {
            if (LOGGER.isDebugEnabled()) {
              LOGGER.debug("measurements being sent:\n{}", measuresAsString);
            }
            channel = DatagramChannel.open();
            ByteBuffer buffer = ByteBuffer.wrap(measuresAsString.getBytes(Miscellaneous.UTF8));
            channel.send(buffer, serverAddress);
            LOGGER.debug("{} measurements sent to UDP[{}:{}]", measures.size(), serverAddress.getHostString(), serverAddress.getPort());
          } catch (IOException e) {
            LOGGER.info("failed to send {} mesures to UDP[{}:{}], {}", measures.size(), serverAddress.getHostString(), serverAddress.getPort(), e.getMessage(), e);
            returnValue = false;
          }
        }
      return returnValue;
    }
}
