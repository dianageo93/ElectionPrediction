# ElectionPrediction
http://event.cwi.nl/lsde/2016/practical2_projects.shtml#W2

## How to run the downloader locally:

```
$ mvn package
$ /path/to/hathi-client/spark/bin/spark-submit --class "downloader.Dowloader" --master local[*] target/download-1.0-SNAPSHOT.jar
```

More on this here: [http://spark.apache.org/docs/latest/submitting-applications.html](http://spark.apache.org/docs/latest/submitting-applications.html)
