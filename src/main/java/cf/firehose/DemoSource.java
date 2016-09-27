package cf.firehose;

import cf.dropsonde.firehose.Firehose;
import org.apache.commons.codec.binary.Hex;
import org.cloudfoundry.dropsonde.events.Envelope;
import org.cloudfoundry.dropsonde.events.UUID;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.integration.annotation.InboundChannelAdapter;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.util.StringUtils;

import java.math.BigInteger;

import static org.cloudfoundry.dropsonde.events.Envelope.EventType;
import static org.cloudfoundry.dropsonde.events.Envelope.EventType.ContainerMetric;
import static org.cloudfoundry.dropsonde.events.Envelope.EventType.LogMessage;

@EnableBinding(Source.class)
@EnableConfigurationProperties(DemoProperties.class)
public class DemoSource {

	@Autowired
	MessageChannel output;

	@Autowired
	Firehose firehose;

	@Autowired
	DemoProperties properties;


	@InboundChannelAdapter(Source.OUTPUT)
	public String adapter() {

		firehose.open().toBlocking().forEach(envelope -> {

			if (envelope.eventType == properties.getLogType()) {
				switch (envelope.eventType) {
					case LogMessage:
						output.send(logMessage(envelope));
						break;
					case Error:
						output.send(errorMessage(envelope));
						break;
					case HttpStartStop:
						output.send(httpStartStopMessage(envelope));
						break;
					case HttpStop:
						output.send(httpStopMessage(envelope));
						break;
					case HttpStart:
						output.send(httpStartMessage(envelope));
						break;
					case CounterEvent:
						output.send(counterEventMessage(envelope));
						break;
					case ContainerMetric:
						output.send(containerMetricMessage(envelope));
						break;
				}
			}
		});

		return null;
	}

	private GenericMessage<String> logMessage(Envelope envelope) {
		return new GenericMessage<String>(String.join("|",
                clean(envelope.origin),
				clean(String.valueOf(envelope.eventType)),
				clean(envelope.timestamp),
				clean(envelope.ip),
                clean(envelope.logMessage.message.utf8()),
				clean(String.valueOf(envelope.logMessage.message_type)),
				clean(envelope.logMessage.app_id),
				clean(envelope.logMessage.source_instance),
				clean(envelope.logMessage.source_type),
				clean(envelope.logMessage.timestamp)


		));
	}

	private Message<?> errorMessage(Envelope envelope) {
		return new GenericMessage<String>(String.join("|",
				clean(envelope.origin),
				clean(String.valueOf(envelope.eventType)),
				clean(envelope.timestamp),
				clean(envelope.ip),
				clean(envelope.error.message),
				clean(envelope.error.source),
				clean(envelope.error.code)
		));
	}

	private Message<?> httpStartMessage(Envelope envelope) {
		return new GenericMessage<String>(String.join("|",
				clean(envelope.origin),
				clean(String.valueOf(envelope.eventType)),
				clean(envelope.timestamp),
				clean(envelope.ip),
				clean(envelope.httpStart.instanceId),
				clean(envelope.httpStart.instanceIndex),
				clean(envelope.httpStart.remoteAddress),
				clean(envelope.httpStart.timestamp),
				clean(envelope.httpStart.uri),
				clean(envelope.httpStart.userAgent),
				clean(getUUID(envelope.httpStart.applicationId)),
				clean(String.valueOf(envelope.httpStart.method)),
				clean(getUUID(envelope.httpStart.parentRequestId)),
				clean(String.valueOf(envelope.httpStart.peerType)),
				clean(getUUID(envelope.httpStart.requestId))
		));
	}

	private Message<?> httpStopMessage(Envelope envelope) {
		return new GenericMessage<String>(String.join("|",
				clean(envelope.origin),
				clean(String.valueOf(envelope.eventType)),
				clean(envelope.timestamp),
				clean(envelope.ip),
				clean(envelope.httpStop.contentLength),
				clean(envelope.httpStop.timestamp),
				clean(envelope.httpStop.uri),
				clean(getUUID(envelope.httpStop.applicationId)),
				clean(String.valueOf(envelope.httpStop.peerType)),
				clean(getUUID(envelope.httpStop.requestId))
		));
	}

	private Message<?> httpStartStopMessage(Envelope envelope) {
		return new GenericMessage<String>(String.join("|",
				clean(envelope.origin),
				clean(String.valueOf(envelope.eventType)),
				clean(envelope.timestamp),
				clean(envelope.ip),
				clean(envelope.httpStartStop.instanceId),
				clean(envelope.httpStartStop.instanceIndex),
				clean(envelope.httpStartStop.remoteAddress),
				clean(String.valueOf(envelope.httpStartStop.startTimestamp)),
				clean(String.valueOf(envelope.httpStartStop.stopTimestamp)),
				clean(envelope.httpStartStop.uri),
				clean(envelope.httpStartStop.userAgent),
				clean(getUUID(envelope.httpStartStop.applicationId)),
				clean(String.valueOf(envelope.httpStartStop.method)),
				clean(String.valueOf(envelope.httpStartStop.peerType)),
				clean(getUUID(envelope.httpStartStop.requestId)),
				clean(envelope.httpStartStop.contentLength),
				clean(envelope.httpStartStop.statusCode),
				clean(String.valueOf(envelope.httpStartStop.forwarded))
		));
	}

	private Message<?> counterEventMessage(Envelope envelope) {
		return new GenericMessage<String>(String.join("|",
				clean(envelope.origin),
				clean(String.valueOf(envelope.eventType)),
				clean(envelope.timestamp),
				clean(envelope.ip),
				clean(envelope.counterEvent.delta),
				clean(envelope.counterEvent.name),
				clean(envelope.counterEvent.total)
		));
	}

	private Message<?> containerMetricMessage(Envelope envelope) {
		return new GenericMessage<String>(String.join("|",
				clean(envelope.origin),
				clean(String.valueOf(envelope.eventType)),
				clean(envelope.timestamp),
				clean(envelope.ip),
				clean(envelope.containerMetric.applicationId),
				clean(envelope.containerMetric.diskBytes),
				clean(envelope.containerMetric.instanceIndex),
				clean(envelope.containerMetric.cpuPercentage),
				clean(envelope.containerMetric.memoryBytes)
		));
	}

	public static String clean(String value) {
		if (StringUtils.isEmpty(value)) return "0";
		return value.replaceAll("\\|"," ").replaceAll("\\\\"," ");
	}

	public static String clean(Long value) {
		if (value == null) return "0";
		return String.valueOf(value);
	}

	public static String clean(Integer value) {
		if (value == null) return "0";
		return String.valueOf(value);
	}

	public static String clean(Double value) {
		if (value == null) return "0";
		return String.valueOf(value);
	}


	public static String getUUID(UUID uuid) {
		byte[] lowBytes = reverse(BigInteger.valueOf(uuid.low).toByteArray());
		byte[] highBytes = reverse(BigInteger.valueOf(uuid.high).toByteArray());
		byte[] data = new byte[lowBytes.length + highBytes.length];

		System.arraycopy(lowBytes, 0, data, 0, lowBytes.length);
		System.arraycopy(highBytes, 0, data, lowBytes.length, highBytes.length);

		StringBuilder builder = new StringBuilder(Hex.encodeHexString(data));
		builder.insert(8,"-").insert(13,"-").insert(18,"-").insert(23,"-");

		return builder.toString();
	}

	private static byte[] reverse(byte[] data) {
		byte[] reversed = new byte[8];
		for(int i=0;i<reversed.length;i++){
			reversed[i] = data[data.length-1-i];
		}
		return reversed;
	}
}
