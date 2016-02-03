package metrics_influxdb.measurements;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.Collection;

import java.lang.Throwable;
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
        DatagramChannel channel = null;
        try {
          channel = DatagramChannel.open();
        } catch (IOException e) {
          LOGGER.info("failed to send {} mesures to UDP[{}:{}], {}", measures.size(), serverAddress.getHostString(), serverAddress.getPort(), e.getMessage(), e);
          returnValue = false;
        }
        int errorsCounter = 0;
        for (Measurement measure: measures) {
          String measuresAsString = inliner.inline(measure);

            if (LOGGER.isDebugEnabled()) {
              LOGGER.debug("measurements being sent:\n{}", measuresAsString);
            }
            ByteBuffer buffer = ByteBuffer.wrap(measuresAsString.getBytes(Miscellaneous.UTF8));
            try {
              channel.send(buffer, serverAddress);
            } catch (Throwable sendException) {
              errorsCounter++;
            }
        }
        LOGGER.debug("{} measurements sent to UDP[{}:{}] with {} errors", measures.size(), serverAddress.getHostString(), serverAddress.getPort(), errorsCounter);

        try {
          channel.disconnect();
        } catch (IOException ioException) {
          LOGGER.error("channel discnnect throws an exception", ioException);
        }
        return returnValue;
    }
}
