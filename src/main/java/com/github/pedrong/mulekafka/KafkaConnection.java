package com.github.pedrong.mulekafka;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.inject.Inject;

import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.mule.api.ConnectionException;
import org.mule.api.MuleContext;
import org.mule.api.annotations.Connect;
import org.mule.api.annotations.ConnectStrategy;
import org.mule.api.annotations.ConnectionIdentifier;
import org.mule.api.annotations.Disconnect;
import org.mule.api.annotations.TestConnectivity;
import org.mule.api.annotations.ValidateConnection;
import org.mule.api.annotations.components.ConnectionManagement;
import org.mule.api.annotations.param.ConnectionKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.pedrong.mulekafka.exception.UnableToLoadPropertiesFile;

@ConnectionManagement(friendlyName = "Connection Configuration", configElementName="connection")
public class KafkaConnection {
	
	private static final Logger logger = LoggerFactory.getLogger(KafkaConnection.class);

	private final AtomicBoolean closed = new AtomicBoolean(false);

	@Inject
	private MuleContext muleContext;

	private String bootstrapServers;
	private String producerPropertiesFile;
	private String consumerPropertiesFile;
	private Producer producer;
	private KafkaConsumer consumer;

    @Connect(strategy = ConnectStrategy.MULTIPLE_INSTANCES)
    @TestConnectivity(active = false)
    public void connect(@ConnectionKey String bootstrapServers, String producerPropertiesFile, String consumerPropertiesFile) throws ConnectionException {
    	if (logger.isDebugEnabled())
    		logger.debug("Connecting to bootstrap Kafka servers {} with properties defined by properties files {} and {}.",
    				bootstrapServers, producerPropertiesFile, consumerPropertiesFile);
    	
    	this.bootstrapServers = bootstrapServers;
    	this.producerPropertiesFile = producerPropertiesFile;
    	this.consumerPropertiesFile = consumerPropertiesFile;
    	
    	Properties producerProperties = getProducerProperties();
    	producer = new KafkaProducer(producerProperties);
    	
        Properties consumerProperties = getConsumerProperties();
        consumer = new KafkaConsumer(consumerProperties);
    }
    
    @Disconnect
    public void disconnect() {if (logger.isDebugEnabled())
		logger.warn("Disconnecting from Kafka servers.");
    			
    	if (!closed.get()) {
    		consumer.wakeup();
    		closed.set(true);
    		producer.close();
    	} else {
    		logger.warn("Connection was already marked as closed.");
    	}
    }
    
    @ConnectionIdentifier
	public String connectionId() {
    	return getBootstrapServers();
	}
    
    @ValidateConnection
    public boolean isValidConnection() {
    	if (consumer == null || producer == null)
    		return false;
    	
    	boolean valid = false;
    	
        try {
			valid = consumer.listTopics() != null;
		} catch (Exception e) {
			valid = false;
		}
        
        if (!valid) {
			try {
				disconnect();
			} catch (Exception e) {
				logger.warn("An exception was thrown while trying to disconnect an invalid Kafka connection.", e);
			}
        }
        
        return valid;
    } 

	public Producer getProducer() {
		return producer;
	}

	public void setProducer(Producer producer) {
		this.producer = producer;
	}

	public KafkaConsumer getConsumer() {
		return consumer;
	}

	public void setConsumer(KafkaConsumer consumer) {
		this.consumer = consumer;
	}

	public String getBootstrapServers() {
		return bootstrapServers;
	}

	public void setBootstrapServers(String bootstrapServers) {
		this.bootstrapServers = bootstrapServers;
	}

	public String getProducerPropertiesFile() {
		return producerPropertiesFile;
	}

	public void setProducerPropertiesFile(String producerPropertiesFile) {
		this.producerPropertiesFile = producerPropertiesFile;
	}

	public String getConsumerPropertiesFile() {
		return consumerPropertiesFile;
	}

	public void setConsumerPropertiesFile(String consumerPropertiesFile) {
		this.consumerPropertiesFile = consumerPropertiesFile;
	}

	public MuleContext getMuleContext() {
		return this.muleContext;
	}

	public void setMuleContext(MuleContext muleContext) {
		this.muleContext = muleContext;
	}

	public void validateConfiguration() {
		validateConsumerProperties();
		validateProducerProperties();
	}

	public Properties getConsumerProperties() {
		Properties consumerProperties = null;
		
		if (StringUtils.isNotEmpty(this.consumerPropertiesFile)) {
			consumerProperties = loadPropertiesFile(this.consumerPropertiesFile);
		} else {
			consumerProperties = new Properties();
		}
		
		consumerProperties = enrichConsumerProperties(consumerProperties);
		return consumerProperties;
	}

	private Properties enrichConsumerProperties(Properties properties) {
		Properties enrichedProperties = new Properties();
		enrichedProperties.putAll(properties);
		enrichPropertiesWithBootstrapServers(enrichedProperties);
		
		if (!enrichedProperties.containsKey("key.deserializer")) {
			enrichedProperties.setProperty("key.deserializer", StringDeserializer.class.getName());
		}
		
		if (!enrichedProperties.containsKey("value.deserializer")) {
			enrichedProperties.setProperty("value.deserializer", StringDeserializer.class.getName());
		}
		
		return enrichedProperties;
	}

	private void enrichPropertiesWithBootstrapServers(Properties enrichedProperties) {
		if ((!enrichedProperties.containsKey("bootstrap.servers")) && (StringUtils.isNotEmpty(this.bootstrapServers))) {
			enrichedProperties.setProperty("bootstrap.servers", this.bootstrapServers);
		}
	}

	private Properties loadPropertiesFile(String propertiesFile) {
		Properties properties = new Properties();
		
		try {
			InputStream propertiesInputStream = getFileInputStream(propertiesFile);
			properties.load(propertiesInputStream);
		} catch (FileNotFoundException e) {
			throw new UnableToLoadPropertiesFile(String.format("Properties file does not exist: %s", new Object[] { propertiesFile }), e);
		} catch (IOException e) {
			throw new UnableToLoadPropertiesFile(String.format("Invalid content of properties file: %s", new Object[] { propertiesFile }), e);
		}
		
		return properties;
	}

	private InputStream getFileInputStream(String fileName) throws FileNotFoundException {
		InputStream inputStream = this.muleContext.getExecutionClassLoader().getResourceAsStream(fileName);
		
		if (inputStream == null) {
			throw new FileNotFoundException(String.format("%s not found.", new Object[] { fileName }));
		}
		
		return inputStream;
	}

	private void validateConsumerProperties() {
		getConsumerProperties();
	}

	private void validateProducerProperties() {
		getProducerProperties();
	}
	
	public boolean isClosed() {
		return closed.get();
	}

	public Properties getProducerProperties() {
		Properties producerProperties = null;
		if (StringUtils.isNotEmpty(this.producerPropertiesFile)) {
			producerProperties = loadPropertiesFile(this.producerPropertiesFile);
		} else {
			producerProperties = new Properties();
		}
		producerProperties = enrichProducerProperties(producerProperties);
		return producerProperties;
	}

	private Properties enrichProducerProperties(Properties properties) {
		Properties enrichedProperties = new Properties();
		enrichedProperties.putAll(properties);
		enrichPropertiesWithBootstrapServers(enrichedProperties);
		if (!enrichedProperties.containsKey("key.serializer")) {
			enrichedProperties.put("key.serializer",
					StringSerializer.class.getName());
		}
		if (!enrichedProperties.containsKey("value.serializer")) {
			enrichedProperties.put("value.serializer",
					StringSerializer.class.getName());
		}
		return enrichedProperties;
	}
	
}
