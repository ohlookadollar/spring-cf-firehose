package cf.firehose;

import cf.dropsonde.firehose.Firehose;
import cf.dropsonde.firehose.FirehoseBuilder;
import org.cloudfoundry.dropsonde.events.Envelope.EventType;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@ConfigurationProperties
public class DemoProperties {

    String logType = "LogMessage";
    String endpoint = "";
    String token = "";

    public EventType getLogType() {
        return EventType.valueOf(logType);
    }

    public String getEndpoint() {
    	return endpoint;
    }

    public String getToken() {
    	return token;
    }

    public void setLogType(String logType) {
        this.logType = logType;
    }
}
