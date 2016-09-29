package sampler;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;

import static org.apache.commons.lang3.StringUtils.containsAny;

public final class Sampler implements Serializable {

    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("Sampler"));

        sc
                .textFile("/input/path")
                // Map each line to ((language, page) -> number_of_visits)).
                .mapToPair((String line) -> {
                    // A line has the form:
                    // language_code page number_of_visits some_other_number
                    // TODO: what is that other number?
                    String[] splits = line.split(" ");
                    if (splits.length >= 3) {
                        return new Tuple2<>(Pair.of(splits[0], splits[1]), Integer.valueOf(splits[2]));
                    } else {
                        System.out.println("==============> " + line);
                        return new Tuple2<>(Pair.of("zz", "UNKNOWN"), Integer.valueOf(1));
                    }
                })
                .filter((Tuple2<Pair<String, String>, Integer> tuple) -> tuple._1().getLeft().contains("en"))
                .filter((Tuple2<Pair<String, String>, Integer> tuple) -> {
                    String wikiItem =  tuple._1().getRight().toLowerCase();
                    return wikiItem.contains("clinton") || wikiItem.contains("trump")
                            || wikiItem.contains("democrat") || wikiItem.contains("republic");
//                    return containsAny(wikiItem, new String[]{"clinton", "trump", "democrat", "republic"});
                })
                // We reduce by the same key summing over the pageviews.
                .reduceByKey((a, b) -> a + b)
                // Remapping into a new object, to print in a csv format.
                .map((Tuple2<Pair<String, String>, Integer> tuple) ->
                        new FileLine(
                                /* language code */ tuple._1().getLeft(),
                                /* wikiItem */ tuple._1().getRight(),
                                /* views */ tuple._2()))
                .saveAsTextFile("/output/path");
    }
}
