package com.smuralee;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public class WordCount {
    private static final Pattern SPACE = Pattern.compile(" ");
    private static final Logger log = LoggerFactory.getLogger(WordCount.class);
    private static final String filePrefix = "wordCounts";
    private static final String newLine = "\n";


    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            log.error("Usage: JavaWordCount <file>");
            System.exit(1);
        }
        SparkConf sparkConf = new SparkConf()
                .setAppName("JavaWordCount")
                .setMaster("local");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = ctx.textFile(args[0], 1);

        JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());
        JavaPairRDD<String, Integer> wordAsTuple = words.mapToPair(word -> new Tuple2<>(word, 1));
        JavaPairRDD<String, Integer> wordWithCount = wordAsTuple.reduceByKey(Integer::sum);
        List<Tuple2<String, Integer>> output = wordWithCount.collect();

        // Create a local file for writing content
        StringBuilder sb = new StringBuilder(filePrefix);
        sb.append(System.currentTimeMillis());
        sb.append(".out.log");

        BufferedWriter writer = new BufferedWriter(new FileWriter(sb.toString(), true));

        for (Tuple2<?, ?> tuple : output) {
            log.info(tuple._1() + ": " + tuple._2());
            writer.append(String.valueOf(tuple._1())).append(": ").append(String.valueOf(tuple._2()));
            writer.append(newLine);
        }

        writer.close();
        ctx.stop();
    }
}
