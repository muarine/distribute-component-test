package com.rtmap.example.flume.sink.kafka;

import com.google.common.base.Throwables;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.kafka.KafkaSinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class RtmapKafkaSink
        extends AbstractSink
        implements Configurable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RtmapKafkaSink.class);
    private Properties kafkaProps;
    private Producer<String, byte[]> producer;
    private String topicList;
    private List<KeyedMessage<String, byte[]>> messageList;
    private int batchSize;
    private KafkaSinkCounter counter;

    public Status process()
            throws EventDeliveryException
    {
        Status result = Status.READY;
        Channel channel = getChannel();
        Transaction transaction = null;
        Event event = null;
        String eventTopic = null;
        String eventKey = null;
        try
        {
            long processedEvents = 0L;

            transaction = channel.getTransaction();
            transaction.begin();

            this.messageList.clear();
            ObjectMapper mapper = new ObjectMapper();
            for (; processedEvents < this.batchSize; processedEvents += 1L)
            {
                event = channel.take();
                if (event == null) {
                    break;
                }
                byte[] eventBody = event.getBody();
                Map<String, String> headers = event.getHeaders();
                eventKey = (String)headers.get("key");
                if (LOGGER.isDebugEnabled())
                {
                    LOGGER.debug("{Event} " + eventTopic + " : " + eventKey + " : " + new String(eventBody, "UTF-8"));

                    LOGGER.debug("event #{}", Long.valueOf(processedEvents));
                }
                String body = new String(eventBody, "UTF-8");
                if(body.contains("KAFKA-DATA:")) {
                    body = body.split("KAFKA-DATA:")[1];
                    if(LOGGER.isDebugEnabled()){
                        LOGGER.debug("body:{}" , body);
                    }
                    JsonNode node = mapper.readTree(body);
                    eventTopic = node.get("head").findValue("topic").getTextValue();
                    if (!topicList.contains(eventTopic)) {
                        LOGGER.warn("this topic:{} is Invalid , please check it or append to flume.properties of topicList.", eventTopic);
                        continue;
                    }
                    KeyedMessage<String, byte[]> data = new KeyedMessage(eventTopic, eventKey, body.getBytes());

                    this.messageList.add(data);
                }
            }
            if (processedEvents > 0L)
            {
                long startTime = System.nanoTime();
                this.producer.send(this.messageList);
                long endTime = System.nanoTime();
                this.counter.addToKafkaEventSendTimer((endTime - startTime) / 1000000L);
                this.counter.addToEventDrainSuccessCount(Long.valueOf(this.messageList.size()).longValue());
            }
            transaction.commit();
        }
        catch (Exception ex)
        {
            String errorMsg = "Failed to publish events";
            LOGGER.error("Failed to publish events", ex);
            result = Status.BACKOFF;
            if (transaction != null) {
                try
                {
                    transaction.rollback();
                    this.counter.incrementRollbackCount();
                }
                catch (Exception e)
                {
                    LOGGER.error("Transaction rollback failed", e);
                    throw Throwables.propagate(e);
                }
            }
            throw new EventDeliveryException(errorMsg, ex);
        }
        finally
        {
            if (transaction != null) {
                transaction.close();
            }
        }
        return result;
    }

    public synchronized void start()
    {
        ProducerConfig config = new ProducerConfig(this.kafkaProps);
        this.producer = new Producer(config);
        this.counter.start();
        super.start();
    }

    public synchronized void stop()
    {
        this.producer.close();
        this.counter.stop();
        LOGGER.info("Kafka Sink {} stopped. Metrics: {}", getName(), this.counter);
        super.stop();
    }

    public void configure(Context context)
    {
        this.batchSize = context.getInteger("batchSize", Integer.valueOf(100)).intValue();
        this.messageList = new ArrayList(this.batchSize);

        this.topicList = context.getString("topicList" , "");

        LOGGER.debug("Using batch size: {}", Integer.valueOf(this.batchSize));

        this.kafkaProps = KafkaSinkUtil.getKafkaProperties(context);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Kafka producer properties: " + this.kafkaProps);
        }
        if (this.counter == null) {
            this.counter = new KafkaSinkCounter(getName());
        }
    }

}
