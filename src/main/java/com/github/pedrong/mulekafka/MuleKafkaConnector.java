package com.github.pedrong.mulekafka;

import java.util.Arrays;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.mule.api.annotations.Config;
import org.mule.api.annotations.Connector;
import org.mule.api.annotations.Processor;
import org.mule.api.annotations.Source;
import org.mule.api.annotations.param.Default;
import org.mule.api.annotations.param.Optional;
import org.mule.api.annotations.param.Payload;
import org.mule.api.callback.SourceCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Connector(name="kafka", friendlyName="Kafka", schemaVersion="1.0", keywords="kafka, topic", minMuleVersion="3.7")
public class MuleKafkaConnector {
	
	private static final Logger logger = LoggerFactory.getLogger(MuleKafkaConnector.class);

    @Config
    private KafkaConnection connection;
    
    public KafkaConnection getConnection() {
		return connection;
	}

	public void setConnection(KafkaConnection connection) {
		this.connection = connection;
	}

	/**
     * Sends message to Kafka topic.
     *
     * @param topic Destination topic.
     * @param messageKey Message key.
     * @param messageBody Message body. Default to #[payload].
     */
    @Processor(name="producer", friendlyName="Producer")
    public void producer(String topic, @Optional String messageKey, @Payload Object messageBody) {
    	if (logger.isDebugEnabled())
    		logger.debug("Sending message to Kafka topic {} with key {} and body {}.", topic, messageKey, messageBody);
    		
    	connection.getProducer().send(new ProducerRecord(topic, messageKey, messageBody));
    }

    /**
     * Subscribe to Kafka topic and poll for messages on a given frequency.
     *
     * @param topic The kafka topic the connector must subscribe.
     * @param pollingFrequency The polling frequency for checking new messages.
     * @throws Exception error produced while processing the payload
     */
    @Source
    public void consumer(SourceCallback callback, String topic, @Optional @Default("1000") int pollingFrequency) throws Exception {
   		logger.info("Subscribing to Kafka topic {} and polling every {} milleseconds.", topic, pollingFrequency);
    	
		connection.getConsumer().subscribe(Arrays.asList(topic));
		
		while (!connection.isClosed()) {
		    ConsumerRecords<Object, Object> records = connection.getConsumer().poll(pollingFrequency);
		 
		    if (records.count() > 0) {
		        for (ConsumerRecord record : records) {
		        	if (logger.isDebugEnabled())
		        		logger.debug("Received message from Kafka: {}.", record);
		        	
		            callback.process(record.value());
		        }
		    }
		}
    }

}