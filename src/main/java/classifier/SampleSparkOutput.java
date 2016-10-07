package classifier;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class SampleSparkOutput implements Serializable {

    public static void main(String[] args) {
        String inputFilePattern = args[0];
        String outputFilePattern = args[1];
        final List<String> keyWords = Arrays.asList(args);
        keyWords.remove(0); // remove inputFilePattern
        keyWords.remove(1); // remove outputFilePattern

        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("SampleSparkOutput"));

        sc
                .textFile(inputFilePattern)
                .filter(new Function<String, Boolean>() {

                    @Override
                    public Boolean call(String s) throws Exception {
                        for (String keyWord : keyWords) {
                            if (s.contains(keyWord)) {
                                return true;
                            }
                        }

                        return false;
                    }
                })
                .repartition(1)
                .saveAsTextFile(outputFilePattern);

        sc.stop();
    }
}
