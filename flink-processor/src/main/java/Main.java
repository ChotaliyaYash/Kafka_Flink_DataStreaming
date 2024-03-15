import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.common.TopicPartition;

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

public class Main {

    static final String BROKERS = "kafka:9092";

    public static void main(String[] args) throws Exception {
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

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

      DataStream<Tuple2<MyAverage, Double>> averageTemperatureStream = kafka.keyBy(myEvent -> myEvent.city)
        .window(TumblingProcessingTimeWindows.of(Time.seconds(60)))
        .aggregate(new AverageAggregator());

      DataStream<Tuple2<String, Double>> cityAndValueStream = averageTemperatureStream
        .map(new MapFunction<Tuple2<MyAverage, Double>, Tuple2<String, Double>>() {
          @Override
          public Tuple2<String, Double> map(Tuple2<MyAverage, Double> input) throws Exception {
            return new Tuple2<>(input.f0.city, input.f1);
          }
        }); 

      cityAndValueStream.print();

      try{

        System.out.println("Connecting to MongoDB");

        MongoSink<Tuple2<String, Double>> sink = MongoSink.<Tuple2<String, Double>>builder()
        .setUri("mongodb+srv://ecommerce:ecommerce@cluster0.xvfq2ua.mongodb.net/my_db?retryWrites=true&w=majority&appName=Cluster0")
        .setDatabase("my_db")
        .setCollection("weather")
        .setBatchSize(1000)
        .setBatchIntervalMs(1000)
        .setMaxRetries(3)
        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .setSerializationSchema((value, context) -> {
            Document doc = new Document("city", value.f0)
                            .append("average_temperature", value.f1);
            return new InsertOneModel<>(BsonDocument.parse(doc.toJson()));
        })
        .build();

        cityAndValueStream.sinkTo(sink);

        System.out.println("MongoDB connected successfully.");
      } catch (Exception e) {
        e.printStackTrace();
      }

      // cityAndValueStream.addSink(JdbcSink.sink("insert into weather (city, average_temperature) values (?, ?)",
      //       (statement, event) -> {
      //         statement.setString(1, event.f0);
      //         statement.setDouble(2, event.f1);
      //       },
      //       JdbcExecutionOptions.builder()
      //         .withBatchSize(1000)
      //         .withBatchIntervalMs(200)
      //         .withMaxRetries(5)
      //         .build(),
      //       new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
      //         .withUrl("jdbc:postgresql://docker.for.mac.host.internal:5438/postgres")
      //         .withDriverName("org.postgresql.Driver")
      //         .withUsername("postgres")
      //         .withPassword("postgres")
      //         .build()
      // ));

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
            myAverage.city = weather.city;
            myAverage.count = myAverage.count + 1;
            myAverage.sum = myAverage.sum + weather.temperature;
            return myAverage;
        }

        @Override
        public Tuple2<MyAverage, Double> getResult(MyAverage myAverage) {
            return new Tuple2<>(myAverage, myAverage.sum / myAverage.count);
        }

        @Override
        public MyAverage merge(MyAverage myAverage, MyAverage acc1) {
            myAverage.sum = myAverage.sum + acc1.sum;
            myAverage.count = myAverage.count + acc1.count;
            return myAverage;
        }
    }

    public static class MyAverage {

        public String city;
        public Integer count = 0;
        public Double sum = 0d;

        @Override
        public String toString() {
            return "MyAverage{" +
                    "city='" + city + '\'' +
                    ", count=" + count +
                    ", sum=" + sum +
                    '}';
        }
    }
}