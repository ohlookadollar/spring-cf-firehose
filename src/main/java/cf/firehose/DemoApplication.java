package cf.firehose;

import cf.dropsonde.firehose.Firehose;
import cf.dropsonde.firehose.FirehoseBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.annotation.InboundChannelAdapter;
import org.springframework.integration.annotation.MessageEndpoint;
import org.springframework.integration.annotation.Poller;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.core.MessageSource;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.scheduling.Trigger;
import org.springframework.scheduling.TriggerContext;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicBoolean;

@SpringBootApplication
@EnableConfigurationProperties(DemoProperties.class)
public class DemoApplication {

	@Autowired
	DemoProperties properties;

	@Bean
	Firehose firehose() {

		String endpoint = properties.getEndpoint();
		String token = properties.getToken();

		return FirehoseBuilder
				.create(endpoint, token)
				.skipTlsValidation(true)
				.build();
	}

	public static void main(String[] args) {
		SpringApplication.run(DemoApplication.class, args);
	}
}
