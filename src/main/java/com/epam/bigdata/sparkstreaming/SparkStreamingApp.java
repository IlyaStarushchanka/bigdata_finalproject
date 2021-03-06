package com.epam.bigdata.sparkstreaming;

/**
 * Created by Ilya_Starushchanka on 10/24/2016.
 */
import com.epam.bigdata.sparkstreaming.entity.CityInfoEntity;
import com.epam.bigdata.sparkstreaming.entity.LogsEntity;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.bitwalker.useragentutils.UserAgent;
import org.apache.spark.SparkConf;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.json.JSONObject;

import java.text.SimpleDateFormat;
import java.util.*;

public class SparkStreamingApp {

    private static final String SPLIT = "\\t";
    private static ObjectMapper mapper = new ObjectMapper();
    private static final SimpleDateFormat LOGS_DATE_FORMAT = new SimpleDateFormat("yyyyMMddhhmmss");
    private static final SimpleDateFormat JSON_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX");

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
        HashMap<Integer, CityInfoEntity> cityInfoMap = new HashMap<>();
        allCities.forEach(city -> {
            String[] fields = city.split(SPLIT);

            /*GeoPoint geoPoint = new GeoPoint();
            geoPoint.resetLat(Double.parseDouble(fields[7]));
            geoPoint.resetLon(Double.parseDouble(fields[6]));*/
            cityInfoMap.put(Integer.parseInt(fields[0]), /*geoPoint*/new CityInfoEntity(Float.parseFloat(fields[6]), Float.parseFloat(fields[7])));
            //cityInfoMap.put(Integer.parseInt(fields[0]), new Float[]{Float.parseFloat(fields[7]), Float.parseFloat(fields[6])}/*new CityInfoEntity(Float.parseFloat(fields[6]), Float.parseFloat(fields[7]))*/);
        });

        SparkConf sparkConf = new SparkConf().setAppName("SparkStreamingLogAggregationApp");
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        sparkConf.set("es.index.auto.create", "true");



        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(2000));

        Broadcast<Map<Integer, CityInfoEntity>> broadcastVar = jssc.sparkContext().broadcast(cityInfoMap);

        Map<String, Integer> topicMap = new HashMap<>();
        for (String topic : topics) {
            topicMap.put(topic, numThreads);
        }

        JavaPairReceiverInputDStream<String, String> messages = KafkaUtils.createStream(jssc, zkQuorum, group, topicMap);


        JavaDStream<String> lines = messages.map(tuple2 -> {
            LogsEntity logsEntity = new LogsEntity(tuple2._2().toString());

            Date date = LOGS_DATE_FORMAT.parse(logsEntity.getTimestamp());
            logsEntity.setTimestamp(JSON_DATE_FORMAT.format(date));

            logsEntity.setGeoPoint(broadcastVar.value().get(logsEntity.getCity()));
            UserAgent ua = UserAgent.parseUserAgentString(tuple2._2().toString());
            String device =  ua.getBrowser() != null ? ua.getOperatingSystem().getDeviceType().getName() : null;
            String osName = ua.getBrowser() != null ? ua.getOperatingSystem().getName() : null;
            String uaFamily = ua.getBrowser() != null ? ua.getBrowser().getGroup().getName() : null;
            logsEntity.setDevice(device);
            logsEntity.setOsName(osName);
            logsEntity.setUaFamily(uaFamily);
            /*String[] fields = tuple2._2().toString().split(SPLIT);

            String json1 = "{\"type\" : \"logs\",\"ipinyour_id\" : \"" + fields[2] +"\"}";*/

            JSONObject jsonObject = new JSONObject(logsEntity);

            jsonObject.append("@sended_at",new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX").format(new Date()));

            String json1 = jsonObject.toString();

            //String json1  = mapper.writeValueAsString(logsEntity);
            System.out.println("####1");

            System.out.println(json1);
            return json1;
        });

        lines.foreachRDD(stringJavaRDD ->
                JavaEsSpark.saveJsonToEs(stringJavaRDD, args[5] + "/" + args[6]/*"logsindext3/logs"*/));

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