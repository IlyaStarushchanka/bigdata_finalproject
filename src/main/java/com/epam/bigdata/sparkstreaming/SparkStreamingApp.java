package com.epam.bigdata.sparkstreaming;

/**
 * Created by Ilya_Starushchanka on 10/24/2016.
 */
import com.epam.bigdata.sparkstreaming.entity.CityInfoEntity;
import com.epam.bigdata.sparkstreaming.entity.LogsEntity;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.net.URI;

public class SparkStreamingApp {

    private static final String SPLIT = "\\t";
    private static ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {

        /*GeoPoint geoPoint = new GeoPoint();
        geoPoint.resetLat(Double.parseDouble("42.626595"));
        geoPoint.resetLon(Double.parseDouble("-0.488439"));
        LogsEntity log = new LogsEntity();
        log.setGeoPoint(geoPoint);
        String json1 = mapper.writeValueAsString(log);
        System.out.println("####1");
        System.out.println(json1);*/

        /*Double[] floats = new Double[]{1.1,2.2,3.3};
        //JSONArray mJSONArray = new JSONArray(Arrays.asList(floats));
        String json1 = mapper.writeValueAsString(floats);
        System.out.println("####1");
        System.out.println(json1);*/


        if (args.length == 0) {
            System.err.println("Usage: SparkStreamingLogAggregationApp {zkQuorum} {group} {topic} {numThreads} {cityPaths}");
            System.exit(1);
        }

        String zkQuorum = args[0];
        String group = args[1];
        String[] topics = args[2].split(",");
        int numThreads = Integer.parseInt(args[3]);

        List<String> allCities = FileHelper.getLinesFromFile(args[4]);
        //HashMap<Integer, Float[]> cityInfoMap = new HashMap<>();
        HashMap<Integer, GeoPoint> cityInfoMap = new HashMap<>();
        allCities.forEach(city -> {
            String[] fields = city.split(SPLIT);
            GeoPoint geoPoint = new GeoPoint();
            geoPoint.resetLat(Double.parseDouble(fields[7]));
            geoPoint.resetLon(Double.parseDouble(fields[6]));
            cityInfoMap.put(Integer.parseInt(fields[0]), geoPoint/*new CityInfoEntity(Float.parseFloat(fields[6]), Float.parseFloat(fields[7]))*/);
            //cityInfoMap.put(Integer.parseInt(fields[0]), new Float[]{Float.parseFloat(fields[7]), Float.parseFloat(fields[6])}/*new CityInfoEntity(Float.parseFloat(fields[6]), Float.parseFloat(fields[7]))*/);
        });

        SparkConf sparkConf = new SparkConf().setAppName("SparkStreamingLogAggregationApp");
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        sparkConf.set("es.index.auto.create", "true");



        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(2000));

        Broadcast<Map<Integer, GeoPoint>> broadcastVar = jssc.sparkContext().broadcast(cityInfoMap);

        Map<String, Integer> topicMap = new HashMap<>();
        for (String topic : topics) {
            topicMap.put(topic, numThreads);
        }

        JavaPairReceiverInputDStream<String, String> messages = KafkaUtils.createStream(jssc, zkQuorum, group, topicMap);

        JavaDStream<String> lines = messages.map(tuple2 -> {
            LogsEntity logsEntity = new LogsEntity(tuple2._2().toString());
            logsEntity.setGeoPoint(broadcastVar.value().get(logsEntity.getCity()));

            /*String[] fields = tuple2._2().toString().split(SPLIT);

            String json1 = "{\"type\" : \"logs\",\"ipinyour_id\" : \"" + fields[2] +"\"}";*/

            //JSONObject jsonObject = new JSONObject(logsEntity);
            String json1  =mapper.writeValueAsString(logsEntity);
            System.out.println("####1");

            System.out.println(json1);
            return json1;
        });

        lines.foreachRDD(stringJavaRDD ->
                JavaEsSpark.saveJsonToEs(stringJavaRDD, "test2/test2"));

//        String json1 = "{\"reason\" : \"business\",\"airport\" : \"SFO\"}";
//        String json2 = "{\"participants\" : 5,\"airport\" : \"OTP\"}";
//
//
//        JavaRDD<String> stringRDD = jssc.parallelize(ImmutableList.of(json1, json2));
//        JavaEsSpark.saveJsonToEs(stringRDD, "spark/json-trips");

        lines.print();

        jssc.start();
        jssc.awaitTermination();
    }
}