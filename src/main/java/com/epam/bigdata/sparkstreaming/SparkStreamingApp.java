package com.epam.bigdata.sparkstreaming;

/**
 * Created by Ilya_Starushchanka on 10/24/2016.
 */
import com.epam.bigdata.sparkstreaming.entity.LogsEntity;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.Map;

public class SparkStreamingApp {

    private static final String SPLIT = "\\t";

    public static void main(String[] args) throws Exception {

        if (args.length == 0) {
            System.err.println("Usage: SparkStreamingLogAggregationApp {zkQuorum} {group} {topic} {numThreads}");
            System.exit(1);
        }

        String zkQuorum = args[0];
        String group = args[1];
        String[] topics = args[2].split(",");
        int numThreads = Integer.parseInt(args[3]);

        SparkConf sparkConf = new SparkConf().setAppName("SparkStreamingLogAggregationApp");
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        sparkConf.set("es.index.auto.create", "true");

        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(2000));

        Map<String, Integer> topicMap = new HashMap<>();
        for (String topic : topics) {
            topicMap.put(topic, numThreads);
        }

        JSONObject jsonObject = new JSONObject("b382c1c156dcbbd5b9317cb50f6a747b    20130606000104000   Vh16OwT6OQNUXbj mozilla/4.0 (compatible; msie 6.0; windows nt 5.1; sv1; qqdownload 718) 180.127.189.*   80  87  1   tFKETuqyMo1mjMp45SqfNX  249b2c34247d400ef1cd3c6bfda4f12a        mm_11402872_1272384_3182279 300 250 1   1   0   00fccc64a1ee2809348509b7ac2a97a5    227 3427    282825712746    0");
        String json1  =jsonObject.toString();
        System.out.println("####1");

        System.out.println(json1);


       JavaPairReceiverInputDStream<String, String> messages = KafkaUtils.createStream(jssc, zkQuorum, group, topicMap);

       /* JavaDStream<String> lines = messages.map(tuple2 -> {
            LogsEntity logsEntity = new LogsEntity(tuple2._2().toString());

            *//*String[] fields = tuple2._2().toString().split(SPLIT);

            String json1 = "{\"type\" : \"logs\",\"ipinyour_id\" : \"" + fields[2] +"\"}";*//*

            JSONObject jsonObject = new JSONObject(logsEntity);
            String json1  =jsonObject.toString();
            System.out.println("####1");

            System.out.println(json1);
            return json1;
        });

        lines.foreachRDD(new VoidFunction<JavaRDD<String>>() {
            @Override
            public void call(JavaRDD<String> stringJavaRDD) throws Exception {
                JavaEsSpark.saveJsonToEs(stringJavaRDD, "test/test");
            }
        });*/

//        String json1 = "{\"reason\" : \"business\",\"airport\" : \"SFO\"}";
//        String json2 = "{\"participants\" : 5,\"airport\" : \"OTP\"}";
//
//
//        JavaRDD<String> stringRDD = jssc.parallelize(ImmutableList.of(json1, json2));
//        JavaEsSpark.saveJsonToEs(stringRDD, "spark/json-trips");

        //lines.print();
        messages.print();
        jssc.start();
        jssc.awaitTermination();
    }
}