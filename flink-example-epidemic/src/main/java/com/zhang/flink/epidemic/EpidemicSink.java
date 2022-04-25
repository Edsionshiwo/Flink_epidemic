package com.zhang.flink.epidemic;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.sql.Timestamp;
import java.time.Duration;

public class EpidemicSink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        KafkaSource<EpidemicModel> source = KafkaSource.<EpidemicModel>builder()
                .setBootstrapServers("hadoop101:9092")
                .setTopics("epidemic2")
                .setGroupId("flinkk")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(new EpidemicModel.EpidemicRecord())
                .build();
        environment
                .setParallelism(1)
                .fromSource(source, WatermarkStrategy
                                .<EpidemicModel>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                .withTimestampAssigner((model, timestamp) -> model.getTime())
                        , "Kafka")

                .keyBy(EpidemicModel::getCity_zipCode)
                .window(TumblingEventTimeWindows.of(Time.days(1)))
//                .allowedLateness(Time.days(30))
                .reduce(new ReduceFunction<EpidemicModel>() {
                    @Override
                    public EpidemicModel reduce(EpidemicModel value1, EpidemicModel value2) throws Exception {
                        return value1.getTime() > value2.getTime() ? value1 : value2;
                    }
                })
                .print();
//        解决方法：https://www.csdn.net/tags/MtTaEg5sMDI5MTU5LWJsb2cO0O0O.html
        environment.execute();

    }
}
