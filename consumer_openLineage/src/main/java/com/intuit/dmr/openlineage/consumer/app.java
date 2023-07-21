package com.intuit.dmr.openlineage.consumer;
//logging done
import org.json.simple.parser.JSONParser;
import org.json.simple.*;
//import org.json.JSONArray;
//import org.json.JSONObject;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.simple.parser.ParseException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.logging.Logger;

public class app
{
    public static final Logger logger= java.util.logging.Logger.getLogger(app.class.getName());
    public static void main(String[] args) {
        // Set Kafka broker address and consumer group ID
        logInit.initializeLogger();
        String bootstrapServers = "broker1a.data-lake-e2e.a.intuit.com:9092,broker1b.data-lake-e2e.a.intuit.com:9092,broker1c.data-lake-e2e.a.intuit.com:9092,broker2a.data-lake-e2e.a.intuit.com:9092,broker2b.data-lake-e2e.a.intuit.com:9092,broker2c.data-lake-e2e.a.intuit.com:9092";
//        String bootstrapServers="localhost:9092";
        String groupId = "mdr_openLineage_test";
        // Set Kafka topic to consume from
        String topic = "dmr_bpp_openlineage_poc";
//        String topic="OpenLineage";
        String protocol = "SSL";
        // Create consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty("security.protocol", protocol);
        properties.setProperty("bootstrap.servers", bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "20971520");
        properties.setProperty(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "20971520");
        logger.info("Consumer Properties: " + "\ntopic:" + topic + "\ngroupId:" + groupId + "\nbootstrapServers:" + bootstrapServers + "\nsecurity protocol:" + protocol + "\nkey deserializer: StringDeserializer" + "value deserializer:StringDeserializer" + "auto_offset_reset:earliest" + "max_partition_fest_bytes:20971520" + "fetch_max_bytes:20971520");
        //System.out.println("Consumer Properties: " + "\ntopic:" + topic + "\ngroupId:" + groupId + "\nbootstrapServers:" + bootstrapServers + "\nsecurity protocol:" + protocol + "\nkey deserializer: StringDeserializer" + "value deserializer:StringDeserializer" + "auto_offset_reset:earliest" + "max_partition_fest_bytes:20971520" + "fetch_max_bytes:20971520");
        // Create Kafka consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        logger.info("Kafka consumer object created");
        //System.out.println("Kafka consumer object created");
        consumer.subscribe(Collections.singletonList(topic));
        logger.info("Consumer subscribed to the topic");
        //System.out.println("Consumer subscribed to the topic");
        // Start consuming messages
        while (true) {
            // Poll for new messages
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(7000));
            logger.info("Polling Done");
           //System.out.println("Polling Done");
            JSONParser parser = new JSONParser();
            // Process each record
            for (ConsumerRecord<String, String> record : records) {
                logger.info("Received event: key=" + record.key() + "value=" + record.value() + "offset:" + record.offset());
                try {
                    JSONObject event = (JSONObject) parser.parse((String) record.value());
                    logger.finer("Processing the json value: calling the createPayLoad() to create the payload");
                    //parse the json events, and create a payload
                    //check if it is a valid event
                    if (!process.isValid(event)) {
                        continue;
                    }
                    JSONObject payload = process.createPayLoad(event);
                    //send the message to DMR
                    postMDR.postAtlas(payload);
                } catch (ParseException e) {
                    logger.severe("Error parsing the JSON event: JSON event object is invalid" + e);
                } catch (hiveMappingException e) {
                    logger.severe("Error creating the payload: Could not find hive table for s3 location");
                } catch (Exception e){
                    logger.severe(e.toString());
                }
            }
            if (records.isEmpty()) {
                logger.info("No event received");
            }
        }
    }
}
