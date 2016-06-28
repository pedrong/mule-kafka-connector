package com.github.pedrong.mulekafka.exception;

public class UnableToLoadPropertiesFile extends KafkaException {
	private static final long serialVersionUID = 7753681017990248293L;

	public UnableToLoadPropertiesFile(String message) {
		super(message);
	}

	public UnableToLoadPropertiesFile(String message, Throwable cause) {
		super(message, cause);
	}
}
