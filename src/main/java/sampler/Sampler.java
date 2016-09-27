package sampler;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;

public final class Sampler implements Serializable {

    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("Sampler"));

        sc
                .textFile("/path/to/samples")
                // Map each line to ((language, page) -> number_of_visits)).
                .mapToPair((String line) -> {
                    // A line has the form:
                    // language_code page number_of_visits some_other_number
                    // TODO: what is that other number?
                    String[] splits = line.split(" ");
                    return new Tuple2<>(Pair.of(splits[0], splits[1]), Integer.valueOf(splits[2]));
                })
                // We only keep those searches in English and those including Hillary or Trump or Sanders.
                .filter((Tuple2<Pair<String, String>, Integer> tuple) -> tuple._1().getLeft().contains("en"))
                // We reduce by the same key summing over the pageviews.
                .reduceByKey((a, b) -> a + b)
                .saveAsTextFile("/path/to/output/folder");
    }
}
