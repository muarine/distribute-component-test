package com.rtmap.example.flume.sink.kafka;

public class KafkaSinkConstants
{
    public static final String PROPERTY_PREFIX = "kafka.";
    public static final String TOPIC = "topic";
    public static final String BATCH_SIZE = "batchSize";
    public static final String MESSAGE_SERIALIZER_KEY = "serializer.class";
    public static final String KEY_SERIALIZER_KEY = "key.serializer.class";
    public static final String BROKER_LIST_KEY = "metadata.broker.list";
    public static final String REQUIRED_ACKS_KEY = "request.required.acks";
    public static final String BROKER_LIST_FLUME_KEY = "brokerList";
    public static final String REQUIRED_ACKS_FLUME_KEY = "requiredAcks";
    public static final int DEFAULT_BATCH_SIZE = 100;
    public static final String DEFAULT_TOPIC = "default-flume-topic";
    public static final String DEFAULT_MESSAGE_SERIALIZER = "kafka.serializer.DefaultEncoder";
    public static final String DEFAULT_KEY_SERIALIZER = "kafka.serializer.StringEncoder";
    public static final String DEFAULT_REQUIRED_ACKS = "1";
}
