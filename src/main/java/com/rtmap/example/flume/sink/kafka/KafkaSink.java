package com.rtmap.example.flume.sink.kafka;

import com.google.common.base.Throwables;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.kafka.KafkaSinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Flume默认自带Kafka-Sink
 */
public class KafkaSink
        extends AbstractSink
        implements Configurable
{
    private static final Logger logger = LoggerFactory.getLogger(KafkaSink.class);
    public static final String KEY_HDR = "key";
    public static final String TOPIC_HDR = "topic";
    private Properties kafkaProps;
    private Producer<String, byte[]> producer;
    private String topic;
    private int batchSize;
    private List<KeyedMessage<String, byte[]>> messageList;
    private KafkaSinkCounter counter;

    public Sink.Status process()
            throws EventDeliveryException
    {
        Sink.Status result = Sink.Status.READY;
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
            for (; processedEvents < this.batchSize; processedEvents += 1L)
            {
                event = channel.take();
                if (event == null) {
                    break;
                }
                byte[] eventBody = event.getBody();
                Map<String, String> headers = event.getHeaders();
                if ((eventTopic = (String)headers.get("topic")) == null) {
                    eventTopic = this.topic;
                }
                eventKey = (String)headers.get("key");
                if (logger.isDebugEnabled())
                {
                    logger.debug("{Event} " + eventTopic + " : " + eventKey + " : " + new String(eventBody, "UTF-8"));

                    logger.debug("event #{}", Long.valueOf(processedEvents));
                }
                KeyedMessage<String, byte[]> data = new KeyedMessage(eventTopic, eventKey, eventBody);

                this.messageList.add(data);
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
            logger.error("Failed to publish events", ex);
            result = Sink.Status.BACKOFF;
            if (transaction != null) {
                try
                {
                    transaction.rollback();
                    this.counter.incrementRollbackCount();
                }
                catch (Exception e)
                {
                    logger.error("Transaction rollback failed", e);
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
        logger.info("Kafka Sink {} stopped. Metrics: {}", getName(), this.counter);
        super.stop();
    }

    public void configure(Context context)
    {
        this.batchSize = context.getInteger("batchSize", Integer.valueOf(100)).intValue();

        this.messageList = new ArrayList(this.batchSize);

        logger.debug("Using batch size: {}", Integer.valueOf(this.batchSize));

        this.topic = context.getString("topic", "default-flume-topic");
        if (this.topic.equals("default-flume-topic")) {
            logger.warn("The Property 'topic' is not set. Using the default topic name: default-flume-topic");
        } else {
            logger.info("Using the static topic: " + this.topic + " this may be over-ridden by event headers");
        }
        this.kafkaProps = KafkaSinkUtil.getKafkaProperties(context);
        if (logger.isDebugEnabled()) {
            logger.debug("Kafka producer properties: " + this.kafkaProps);
        }
        if (this.counter == null) {
            this.counter = new KafkaSinkCounter(getName());
        }
    }
}
