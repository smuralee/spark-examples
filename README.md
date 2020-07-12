# Spark example

This sample program can be used for testing the Spark cluster. I have used this for testing the EMR cluster with Spark application installed.

## Build

* Maven is used for dependency management and building the application
* The compiled jar uses the `shade plugin` and has `main class` - `com.smuralee.WordCount`
* It is built into the `target` folder - `spark-examples-1.0.jar`

```
mvn clean install
```

## Run the application

* A sample `input-file.txt` is provided in the `resources` folder within the `src`
* Create an S3 bucket in the AWS account containing the EMR cluster and use the `bucketName` as the second argument
* The output file `wordCounts.out.log` will be uploaded to the S3 bucket

```
java -jar spark-examples-1.0.jar input-file.txt output-s3-bucket-123456789012
```


