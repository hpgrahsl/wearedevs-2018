package com.github.hpgrahsl.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.BufferedReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.Random;

public class FallbackTweetsProducer {

    public static Random RNG = new Random();
    public static KafkaProducer<String, String> producer;

    public static void main(String[] args) {

        if(args.length != 4) {
            System.err.println("error: provide 4 args <broker_url> <path_to_file> <topic_name> <max_delay_ms>");
            System.exit(-1);
        }

        System.out.println("Press CTRL-C to stop tweet stream API simulation...");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("shutting down application and closing producer");
            if (producer != null)
                producer.close();
        }));

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, args[0]);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<>(props);

        try (BufferedReader br = Files.newBufferedReader(
                Paths.get(args[1]), StandardCharsets.UTF_8)
        ) {
            String tweet;
            while ((tweet = br.readLine()) != null) {
                ProducerRecord<String, String> record = new ProducerRecord<>(args[2], tweet);
                System.out.println("producer sends -> "+tweet);
                producer.send(record, (RecordMetadata r, Exception e) -> {
                    if (e != null) {
                        System.err.println("error: failed while writing tweet to topic " + r.topic());
                        e.printStackTrace();
                    }
                });
                Thread.sleep(RNG.nextInt(Integer.parseInt(args[3])));
            }
        } catch (Exception exc) {
            exc.printStackTrace();
        }

    }

}
