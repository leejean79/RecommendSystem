/*
 * Copyright (c) 2017. WuYufei All rights reserved.
 */

package com.leejean;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TopologyBuilder;

import java.util.Properties;

/**
 * Created by wuyufei on 2017/6/18.
 */
public class Application {
    public static void main(String[] args){

//        if (args.length < 4) {
//            System.err.println("Usage: kafkaStream <brokers> <zookeepers> <from> <to>\n" +
//                    "  <brokers> is a list of one or more Kafka brokers\n" +
//                    "  <zookeepers> is a list of one or more Zookeeper nodes\n" +
//                    "  <from> is a topic to consume from\n" +
//                    "  <to> is a topic to product to\n\n");
//            System.exit(1);
//        }
//        String brokers = args[0];
//        String zookeepers = args[1];
//        String from = args[2];
//        String to = args[3];

        String input = "abc";
        String output = "recommender";

        Properties settings = new Properties();
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "logProcessor");
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "f5903:9092");
        settings.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "f5903:2181");
//        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
//        settings.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, zookeepers);

        StreamsConfig config = new StreamsConfig(settings);

        TopologyBuilder builder = new TopologyBuilder();

        //kafka管道的结构定义
        builder.addSource("SOURCE", input)
                .addProcessor("PROCESS", () -> new LogProcessor(), "SOURCE")
                .addSink("SINK", output, "PROCESS");

        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.start();
    }
}
