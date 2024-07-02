import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.common.TopicPartition;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.JobManagerOptions;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.Arrays;
import java.util.HashSet;

import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.mongodb.sink.MongoSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import com.mongodb.client.model.InsertOneModel;
import org.bson.BsonDocument;
import org.bson.Document;

import java.util.Date;


// {
//   "tagId": "",
//   "channelId": "",
//   "publisherId": "",
//   "adsSourceId": "",
//   "publisherChannelId": "",
//   "connectionId": "",
//   "inventory": 0,
//   "timestamp": "2020-01-01T00:00:00.000Z",
// }

public class Main {

    static final String BROKERS = "kafka:9092";

    public static void main(String[] args) throws Exception {


        Configuration config = new Configuration();

        // Set TaskManager memory
        // config.set(TaskManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.ofMebiBytes(4096));
        // Set JobManager memory
        // config.set(JobManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.ofMebiBytes(4096));

        // config.setFloat("taskmanager.memory.network.fraction", 0.2f);
        // config.setString("taskmanager.memory.network.min", "64mb");
        // config.setString("taskmanager.memory.network.max", "1gb");
        // config.setString("taskmanager.memory.process.size", "4096mb");
        // config.setInteger("taskmanager.numberOfTaskSlots", 4);

      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);

      System.out.println("Environment created");
      KafkaSource<Weather> source = KafkaSource.<Weather>builder()
                                      .setBootstrapServers(BROKERS)
                                      .setProperty("partition.discovery.interval.ms", "20000")
                                      .setTopics("weather")
                                      .setGroupId("groupdId-919292")
                                      .setStartingOffsets(OffsetsInitializer.earliest())
                                      .setValueOnlyDeserializer(new WeatherDeserializationSchema())
                                      .build();

    DataStreamSource<Weather> kafka = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka");

    System.out.println("Kafka source created");
    kafka.print();

    //   for tagID
    DataStream<Tuple2<MyAverage, Double>> tagIdDataStream = kafka.keyBy(myEvent -> myEvent.tagId)
    .window(TumblingProcessingTimeWindows.of(Time.seconds(60)))
    .aggregate(new AverageAggregator());

    DataStream<WeatherData> tagIdData = tagIdDataStream
        .map(new MapFunction<Tuple2<MyAverage, Double>, WeatherData>() {
            @Override
            public WeatherData map(Tuple2<MyAverage, Double> input) throws Exception {
                System.out.println(input.f0.toString() + " " + input.f1);
                return new WeatherData(input.f0.tagId, input.f0.channelId, input.f0.publisherId, input.f0.adsSourceId, input.f0.publisherChannelId, input.f0.connectionId, input.f1);
            }
        });

    //   for channelId
    DataStream<Tuple2<MyAverage, Double>> channelIdDataStream = kafka.keyBy(myEvent -> myEvent.channelId)
        .window(TumblingProcessingTimeWindows.of(Time.seconds(60)))
        .aggregate(new AverageAggregator());

    DataStream<WeatherData> channelIdData = channelIdDataStream
        .map(new MapFunction<Tuple2<MyAverage, Double>, WeatherData>() {
            @Override
            public WeatherData map(Tuple2<MyAverage, Double> input) throws Exception {
                System.out.println(input.f0.toString() + " " + input.f1);
                return new WeatherData(input.f0.tagId, input.f0.channelId, input.f0.publisherId, input.f0.adsSourceId, input.f0.publisherChannelId, input.f0.connectionId, input.f1);
            }
        });

    //   for adsSourceId
    DataStream<Tuple2<MyAverage, Double>> adsSourceIdDataStream = kafka.keyBy(myEvent -> myEvent.adsSourceId)
        .window(TumblingProcessingTimeWindows.of(Time.seconds(60)))
        .aggregate(new AverageAggregator());

    DataStream<WeatherData> adsSourceIdData = adsSourceIdDataStream
        .map(new MapFunction<Tuple2<MyAverage, Double>, WeatherData>() {
            @Override
            public WeatherData map(Tuple2<MyAverage, Double> input) throws Exception {
                System.out.println(input.f0.toString() + " " + input.f1);
                return new WeatherData(input.f0.tagId, input.f0.channelId, input.f0.publisherId, input.f0.adsSourceId, input.f0.publisherChannelId, input.f0.connectionId, input.f1);
            }
        });

    //   for publisherChannelId
    DataStream<Tuple2<MyAverage, Double>> publisherChannelIdDataStream = kafka.keyBy(myEvent -> myEvent.publisherChannelId)
        .window(TumblingProcessingTimeWindows.of(Time.seconds(60)))
        .aggregate(new AverageAggregator());

    DataStream<WeatherData> publisherChannelIdData = publisherChannelIdDataStream
        .map(new MapFunction<Tuple2<MyAverage, Double>, WeatherData>() {
            @Override
            public WeatherData map(Tuple2<MyAverage, Double> input) throws Exception {
                System.out.println(input.f0.toString() + " " + input.f1);
                return new WeatherData(input.f0.tagId, input.f0.channelId, input.f0.publisherId, input.f0.adsSourceId, input.f0.publisherChannelId, input.f0.connectionId, input.f1);
            }
        });

    //   for connectionId
    DataStream<Tuple2<MyAverage, Double>> connectionIdDataStream = kafka.keyBy(myEvent -> myEvent.connectionId)
        .window(TumblingProcessingTimeWindows.of(Time.seconds(60)))
        .aggregate(new AverageAggregator());
    
    DataStream<WeatherData> connectionIdData = connectionIdDataStream
        .map(new MapFunction<Tuple2<MyAverage, Double>, WeatherData>() {
            @Override
            public WeatherData map(Tuple2<MyAverage, Double> input) throws Exception {
                System.out.println(input.f0.toString() + " " + input.f1);
                return new WeatherData(input.f0.tagId, input.f0.channelId, input.f0.publisherId, input.f0.adsSourceId, input.f0.publisherChannelId, input.f0.connectionId, input.f1);
            }
        });

      try{
        System.out.println("Connecting to MongoDB");

        //  for tagId
        MongoSink<WeatherData> tagSink = MongoSink.<WeatherData>builder()
            .setUri("mongodb+srv://nandpatel1292:yogaApp@cluster0.srwkvb4.mongodb.net/yoga?retryWrites=true&w=majority")
            .setDatabase("my_db")
            .setCollection("tag")
            .setBatchSize(1000)
            .setBatchIntervalMs(1000)
            .setMaxRetries(3)
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .setSerializationSchema((value, context) -> {
                Document doc = new Document("tagId", value.tagId)
                                .append("inventory_count", value.inventory)
                                .append("createdAt", new Date().getTime());
                return new InsertOneModel<>(BsonDocument.parse(doc.toJson()));
            })
            .build();
        
        tagIdData.sinkTo(tagSink);
        
        //  for channelId
        MongoSink<WeatherData> channelSink = MongoSink.<WeatherData>builder()
            .setUri("mongodb+srv://nandpatel1292:yogaApp@cluster0.srwkvb4.mongodb.net/yoga?retryWrites=true&w=majority")
            .setDatabase("my_db")
            .setCollection("channel")
            .setBatchSize(1000)
            .setBatchIntervalMs(1000)
            .setMaxRetries(3)
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .setSerializationSchema((value, context) -> {
                Document doc = new Document("channelId", value.channelId)
                                .append("inventory_count", value.inventory)
                                .append("createdAt", new Date().getTime());
                return new InsertOneModel<>(BsonDocument.parse(doc.toJson()));
            })
            .build();

        channelIdData.sinkTo(channelSink);

        //  for adsSourceId
        MongoSink<WeatherData> adSourceSink = MongoSink.<WeatherData>builder()
            .setUri("mongodb+srv://nandpatel1292:yogaApp@cluster0.srwkvb4.mongodb.net/yoga?retryWrites=true&w=majority")
            .setDatabase("my_db")
            .setCollection("adsSource")
            .setBatchSize(1000)
            .setBatchIntervalMs(1000)
            .setMaxRetries(3)
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .setSerializationSchema((value, context) -> {
                Document doc = new Document("adsSourceId", value.adsSourceId)
                                .append("inventory_count", value.inventory)
                                .append("createdAt", new Date().getTime());
                return new InsertOneModel<>(BsonDocument.parse(doc.toJson()));
            })
            .build();

        adsSourceIdData.sinkTo(adSourceSink);
        
        //  for publisherChannelId
        MongoSink<WeatherData> publisherChannelSink = MongoSink.<WeatherData>builder()
            .setUri("mongodb+srv://nandpatel1292:yogaApp@cluster0.srwkvb4.mongodb.net/yoga?retryWrites=true&w=majority")
            .setDatabase("my_db")
            .setCollection("publisherChannel")
            .setBatchSize(1000)
            .setBatchIntervalMs(1000)
            .setMaxRetries(3)
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .setSerializationSchema((value, context) -> {
                Document doc = new Document("publisherChannelId", value.publisherChannelId)
                                .append("inventory_count", value.inventory)
                                .append("createdAt", new Date().getTime());
                return new InsertOneModel<>(BsonDocument.parse(doc.toJson()));
            })
            .build();

        publisherChannelIdData.sinkTo(publisherChannelSink);
        
        //  for connectionId
        MongoSink<WeatherData> connectionSink = MongoSink.<WeatherData>builder()
            .setUri("mongodb+srv://nandpatel1292:yogaApp@cluster0.srwkvb4.mongodb.net/yoga?retryWrites=true&w=majority")
            .setDatabase("my_db")
            .setCollection("connection")
            .setBatchSize(1000)
            .setBatchIntervalMs(1000)
            .setMaxRetries(3)
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .setSerializationSchema((value, context) -> {
                Document doc = new Document("connectionId", value.connectionId)
                                .append("inventory_count", value.inventory)
                                .append("createdAt", new Date().getTime());
                return new InsertOneModel<>(BsonDocument.parse(doc.toJson()));
            })
            .build();

        connectionIdData.sinkTo(connectionSink);


        System.out.println("MongoDB connected successfully.");
      } catch (Exception e) {
        e.printStackTrace();
      }

      env.execute("Kafka-flink-postgres");
    }

    /**
     * Aggregation function for average.
     */
    public static class AverageAggregator implements AggregateFunction<Weather, MyAverage, Tuple2<MyAverage, Double>> {

        @Override
        public MyAverage createAccumulator() {
            return new MyAverage();
        }

        @Override
        public MyAverage add(Weather weather, MyAverage myAverage) {
            //logger.debug("add({},{})", myAverage.city, myEvent);
            myAverage.tagId = weather.tagId;
            myAverage.channelId = weather.channelId;
            myAverage.publisherId = weather.publisherId;
            myAverage.adsSourceId = weather.adsSourceId;
            myAverage.publisherChannelId = weather.publisherChannelId;
            myAverage.connectionId = weather.connectionId;
            myAverage.count = myAverage.count + 1;
            return myAverage;
        }

        @Override
        public Tuple2<MyAverage, Double> getResult(MyAverage myAverage) {
            return new Tuple2<>(myAverage, myAverage.count);
        }

        @Override
        public MyAverage merge(MyAverage myAverage, MyAverage acc1) {
            myAverage.count = myAverage.count + acc1.count;
            return myAverage;
        }
    }

    public static class MyAverage {

        public String tagId;
        public String channelId;
        public String publisherId;
        public String adsSourceId;
        public String publisherChannelId;
        public String connectionId;
        public Double count = 0.0;

        @Override
        public String toString() {
            return "MyAverage{" +
                    "tagId='" + tagId + '\'' +
                    ", channelId='" + channelId + '\'' +
                    ", publisherId='" + publisherId + '\'' +
                    ", adsSourceId='" + adsSourceId + '\'' +
                    ", publisherChannelId='" + publisherChannelId + '\'' +
                    ", connectionId='" + connectionId + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}