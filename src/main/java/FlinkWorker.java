import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.Producer;
import twitter4j.HashtagEntity;
import twitter4j.Status;
import twitter4j.TwitterObjectFactory;
import twitter4j.UserMentionEntity;

import java.util.ArrayList;
import java.util.Date;
import java.util.Properties;

public class FlinkWorker{

    private static Producer<String, String> producer;
    private static FlinkKafkaProducer011<String> hashtagProducer;
    private static FlinkKafkaProducer011<String> mentionProducer;
    private static int count = 0;

    final static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("acks", "all");
        properties.put("retries", 0);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);

        hashtagProducer = new FlinkKafkaProducer011<String>(
                "HashtagStream", (SerializationSchema) new SimpleStringSchema(), properties);
        mentionProducer = new FlinkKafkaProducer011<String>(
                "MentionStream", (SerializationSchema) new SimpleStringSchema(), properties);

        process();
    }

    public static void process() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("zookeeper.connect", "localhost:2181");
        properties.setProperty("group.id", "FlinkWorker");
        FlinkKafkaConsumer011<String> twitterConsumer = new FlinkKafkaConsumer011<>("TweetStream", new SimpleStringSchema(), properties);
        twitterConsumer.setStartFromEarliest();

        while(true) {
            DataStreamSource<String> input = env.addSource(twitterConsumer);

            DataStream<Status> mappedInput = input.map((MapFunction<String, Status>) s -> {
                Status status = TwitterObjectFactory.createStatus(s);
                return status;
            });

            mappedInput = mappedInput.filter(status -> (status.getUserMentionEntities().length >= 1 || status.getHashtagEntities().length >= 1));

            DataStream<HibernateEntity> entities = mappedInput.flatMap(new FlatMapFunction<Status, HibernateEntity>() {
                @Override
                public void flatMap(Status status, Collector<HibernateEntity> collector) throws Exception {
                    Date date = status.getCreatedAt();
                    for(HashtagEntity hashtagEntity : status.getHashtagEntities()) {
                        Hashtag hashtag = new Hashtag();
                        hashtag.setTag(hashtagEntity.getText());
                        hashtag.setTime(date);
                        collector.collect(hashtag);
                    }
                    for(UserMentionEntity mentionEntity : status.getUserMentionEntities()){
                        Mention mention = new Mention();
                        mention.setName(mentionEntity.getScreenName());
                        mention.setTime(date);
                        collector.collect(mention);
                    }
                }
            });

            SplitStream<HibernateEntity> split = entities.split((OutputSelector<HibernateEntity>) hibernateEntity -> {
                ArrayList<String> list = new ArrayList<>();
                if(hibernateEntity instanceof  Hashtag) list.add("hashtag");
                else if (hibernateEntity instanceof  Mention) list.add("mention");
                return list;
            });

            DataStream<HibernateEntity> hashtags = split.select("hashtag");
            DataStream<HibernateEntity> mentions = split.select("mention");

            ObjectMapper mapper = new ObjectMapper();
            DataStream<String> hashtagJSONs = hashtags.map(entity -> mapper.writeValueAsString(entity));
            DataStream<String> mentionJSONs = mentions.map(entity -> mapper.writeValueAsString(entity));


            hashtagJSONs.addSink(hashtagProducer);
            mentionJSONs.addSink(mentionProducer);

            try {
                env.execute();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
