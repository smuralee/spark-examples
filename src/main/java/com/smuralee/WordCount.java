package com.smuralee;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public class WordCount {
    private static final Pattern SPACE = Pattern.compile(" ");
    private static final Logger log = LoggerFactory.getLogger(WordCount.class);
    private static final String outFileName = "wordCounts.out.log";
    private static final String newLine = "\n";


    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            log.error("Usage: JavaWordCount <file>");
            log.error("Usage: Output destination <S3 bucket>");
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

        BufferedWriter writer = new BufferedWriter(new FileWriter(outFileName, true));

        for (Tuple2<?, ?> tuple : output) {
            log.info(tuple._1() + ": " + tuple._2());
            writer.append(String.valueOf(tuple._1())).append(": ").append(String.valueOf(tuple._2()));
            writer.append(newLine);
        }

        writer.close();

        //Upload file to S3
        final String bucketName = args[1];

        S3Client s3 = S3Client.builder().build();
        s3.putObject(
                PutObjectRequest.builder()
                        .bucket(bucketName)
                        .key(outFileName)
                        .build(), new File(outFileName).toPath()
        );

        ctx.stop();
    }
}
