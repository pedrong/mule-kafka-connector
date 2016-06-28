package com.github.pedrong.mulekafka.exception;

public class KafkaException extends RuntimeException {

	private static final long serialVersionUID = -3034143838882051054L;

	public KafkaException(String message) {
		super(message);
	}

	public KafkaException(String message, Throwable cause) {
		super(message, cause);
	}
}
