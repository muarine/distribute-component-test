package com.rtmap.example.flume.sink.kafka;

import org.apache.flume.Context;
import org.apache.flume.conf.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

/**
 * KafkaSink工具类
 */
public class KafkaSinkUtil
{
    private static final Logger log = LoggerFactory.getLogger(KafkaSinkUtil.class);

    public static Properties getKafkaProperties(Context context)
    {
        log.info("context={}", context.toString());
        Properties props = generateDefaultKafkaProps();
        setKafkaProps(context, props);
        addDocumentedKafkaProps(context, props);
        return props;
    }

    private static void addDocumentedKafkaProps(Context context, Properties kafkaProps)
            throws ConfigurationException
    {
        String brokerList = context.getString("brokerList");
        if (brokerList == null) {
            throw new ConfigurationException("brokerList must contain at least one Kafka broker");
        }
        kafkaProps.put("metadata.broker.list", brokerList);

        String requiredKey = context.getString("requiredAcks");
        if (requiredKey != null) {
            kafkaProps.put("request.required.acks", requiredKey);
        }
    }

    private static Properties generateDefaultKafkaProps()
    {
        Properties props = new Properties();
        props.put("serializer.class", "kafka.serializer.DefaultEncoder");

        props.put("key.serializer.class", "kafka.serializer.StringEncoder");

        props.put("request.required.acks", "1");

        return props;
    }

    private static void setKafkaProps(Context context, Properties kafkaProps)
    {
        Map<String, String> kafkaProperties = context.getSubProperties("kafka.");
        for (Map.Entry<String, String> prop : kafkaProperties.entrySet())
        {
            kafkaProps.put(prop.getKey(), prop.getValue());
            if (log.isDebugEnabled()) {
                log.debug("Reading a Kafka Producer Property: key: " + (String)prop.getKey() + ", value: " + (String)prop.getValue());
            }
        }
    }
}
