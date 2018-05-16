package com.github.hpgrahsl.kafka;

import com.github.hpgrahsl.kafka.config.KStreamsProperties;
import com.github.hpgrahsl.kafka.emoji.EmojiUtils;
import com.github.hpgrahsl.kafka.model.EmojiCount;
import com.github.hpgrahsl.kafka.model.TopEmojis;
import com.github.hpgrahsl.kafka.model.Tweet;
import com.github.hpgrahsl.kafka.serde.EmojiCountSerde;
import com.github.hpgrahsl.kafka.serde.TopNSerdeEC;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Properties;

@SpringBootApplication
@EnableConfigurationProperties(KStreamsProperties.class)
public class KStreamsEmojiTracker {

    private static final Logger LOGGER = LoggerFactory.getLogger(KStreamsEmojiTracker.class);

    @Bean
    public Topology kStreamsTopology(KStreamsProperties config) {

        Serde<String> stringSerde = Serdes.String();
        Serde<EmojiCount> countSerde = new EmojiCountSerde();
        Serde<TopEmojis> topNSerde = new TopNSerdeEC(config.getEmojiCountTopN());

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Tweet> tweets = builder.stream(config.getTweetsTopic());

        //STATEFUL COUNTING OF ALL EMOJIS CONTAINED IN THE TWEETS
        KTable<String, Long> emojiCounts = tweets
            .map((id,tweet) -> KeyValue.pair(id,EmojiUtils.extractEmojisAsString(tweet.Text)))
            .flatMapValues(emojis -> emojis)
            .map((id,emoji) -> KeyValue.pair(emoji,""))
            .groupByKey(Serialized.with(stringSerde, stringSerde))
            .count(Materialized.as(Stores.inMemoryKeyValueStore(config.getStateStoreEmojiCounts())));

        emojiCounts.toStream().foreach((emoji,count) -> LOGGER.debug(emoji + "  "+count));

        //MAINTAIN OVERALL TOP N EMOJI SEEN SO FAR
        KTable<String, TopEmojis> mostFrequent = emojiCounts.toStream()
            .map((e, cnt) -> KeyValue.pair("topN", new EmojiCount(e, cnt)))
            .groupByKey(Serialized.with(stringSerde, countSerde))
            .aggregate(
                () -> new TopEmojis(config.getEmojiCountTopN()),
                (key, emojiCount, topEmojis) -> {
                    topEmojis.add(emojiCount);
                    return topEmojis;
                },
                Materialized.
                        as(Stores.inMemoryKeyValueStore(config.getStateStoreEmojisTopN()))
                            .withValueSerde((Serde)topNSerde)
            );

        //mostFrequent.toStream().foreach((k,topN) -> LOGGER.debug(topN.toString()));

        //FOR EACH UNIQUE EMOJI WITHIN A TWEET, EMIT & STORE THE TWEET ONCE
        tweets
            .map((id,tweet) -> KeyValue.pair(tweet.Text,new LinkedHashSet<>(EmojiUtils.extractEmojisAsString(tweet.Text))))
            .flatMapValues(uniqueEmojis -> uniqueEmojis)
            .map((text,emoji) -> KeyValue.pair(emoji, text))
            //.peek((emoji,text) -> LOGGER.debug(emoji + "  "+ text))
            .to(config.getEmojiTweetsTopic(), Produced.with(stringSerde,stringSerde));

        return builder.build();

    }

    @Bean
    public KafkaStreams kafkaStreams(KStreamsProperties config) {
        Properties props = new Properties();
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, config.getConsumerAutoOffsetReset());
        props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, config.getApplicationServer());
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, config.getApplicationId());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, config.getDefaultKeySerde());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, config.getDefaultValueSerde());
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, config.getProcessingGuarantee());
        props.put(StreamsConfig.STATE_DIR_CONFIG, config.getStateStoreDirectory());
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, config.getCacheMaxBytesBuffer());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, config.getCommitIntervalMs());
        props.put(CommonClientConfigs.METADATA_MAX_AGE_CONFIG, config.getMetadataMaxAgeMs());
        return new KafkaStreams(kStreamsTopology(config), props);
    }

    @Component
    public static class KafkaStreamsBootstrap implements CommandLineRunner {

        @Autowired
        KafkaStreams kafkaStreams;

        @Override
        public void run(String... args) {
            kafkaStreams.cleanUp();
            kafkaStreams.start();
            Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
        }
    }

    public static void main(String[] args) {
        SpringApplication.run(KStreamsEmojiTracker.class);
    }

}
