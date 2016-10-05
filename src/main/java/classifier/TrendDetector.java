package classifier;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaNewHadoopRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.*;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.Double.parseDouble;
import static java.lang.Integer.parseInt;
import static java.util.Arrays.asList;

public final class TrendDetector implements Serializable {
    private static final String CONFIG_FILE_PATH = "/config.properties";
    private static final String SERIES_LENGTH = "seriesLength";
    private static final String REFERENCE_LENGTH = "referenceLength";
    private static final String LAMBDA = "lambda";
    private static final String BASELINE_OFFSET = "baselineOffset";
    private static final String N_SMOOTH = "nSmooth";
    private static final String ALPHA = "alpha";

    public static void main(String[] args) throws IOException, ClassNotFoundException {
        String inputFilePattern = args[0];
        String outputFilePattern = args[1];



        InputStream inputStream = TrendDetector.class.getResourceAsStream(CONFIG_FILE_PATH);
        checkState(inputStream != null, "Could not find config file in class resources.");
        Properties properties = new Properties();
        properties.load(inputStream);

        ReferenceTrends referenceTrends =
                new ReferenceTrends(
                        parseInt(properties.getProperty(REFERENCE_LENGTH)),
                        parseInt(properties.getProperty(BASELINE_OFFSET)),
                        parseInt(properties.getProperty(N_SMOOTH)),
                        parseDouble(properties.getProperty(ALPHA)));

        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("TrendDetector"));

        JavaPairRDD<LongWritable, Text> javaPairRDD = sc.newAPIHadoopFile(
                inputFilePattern,
                TextInputFormat.class,
                LongWritable.class,
                Text.class,
                new Configuration()
        );
        JavaNewHadoopRDD<LongWritable, Text> hadoopRDD = (JavaNewHadoopRDD) javaPairRDD;
        JavaRDD<Tuple2<String, String>> namedLinesRDD = hadoopRDD.mapPartitionsWithInputSplit(
                new Function2<InputSplit, Iterator<Tuple2<LongWritable, Text>>, Iterator<Tuple2<String, String>>>() {
                    @Override
                    public Iterator<Tuple2<String, String>> call(InputSplit inputSplit, final Iterator<Tuple2<LongWritable, Text>> lines) throws Exception {
                        FileSplit fileSplit = (FileSplit) inputSplit;
                        final String fileName = fileSplit.getPath().getName();
                        return new Iterator<Tuple2<String, String>>() {
                            @Override
                            public boolean hasNext() {
                                return lines.hasNext();
                            }
                            @Override
                            public Tuple2<String, String> next() {
                                Tuple2<LongWritable, Text> entry = lines.next();
                                return new Tuple2<String, String>(fileName, entry._2().toString());
                            }
                            @Override
                            public void remove() {

                            }
                        };
                    }
                },
                true
        );

        namedLinesRDD
                .flatMapToPair(new TopicToTimeseries())
                .reduceByKey(new OrderTimeseriesByTimestamp())
                .mapValues(new ComputeEta(properties, referenceTrends))
                .saveAsTextFile(outputFilePattern);

        sc.stop();
    }

    private static final class ComputeEta implements
            Function<TreeMap<Long,Double>, TreeMap<Long, Tuple2<Double, Double>>> {
        private final WeightedDataTemplates dataTemplate;

        public ComputeEta(Properties properties, ReferenceTrends referenceTrends) {
            this.dataTemplate =
                    new WeightedDataTemplates(
                            parseInt(properties.getProperty(SERIES_LENGTH)),
                            parseInt(properties.getProperty(REFERENCE_LENGTH)),
                            parseDouble(properties.getProperty(LAMBDA)),
                            referenceTrends);
        }

        @Override
        public TreeMap<Long, Tuple2<Double, Double>> call(TreeMap<Long, Double> input) throws Exception {
            TreeMap<Long, Tuple2<Double, Double>> result = new TreeMap<>();
            for (Map.Entry<Long, Double> it : input.entrySet()) {
                dataTemplate.update(it.getValue());
                result.put(it.getKey(), new Tuple2<>(it.getValue(), dataTemplate.getResult()));
            }

            return result;
        }
    }

    private static final class OrderTimeseriesByTimestamp implements
            Function2<TreeMap<Long, Double>, TreeMap<Long, Double>, TreeMap<Long, Double>> {

        @Override
        public TreeMap<Long, Double> call(TreeMap<Long, Double> map1, TreeMap<Long, Double> map2) throws Exception {
            TreeMap<Long, Double> result = new TreeMap<>();
            result.putAll(map1);
            result.putAll(map2);
            return result;
        }
    }

    private static final class TopicToTimeseries implements
            PairFlatMapFunction<Tuple2<String,String>, String, TreeMap<Long, Double>> {

        @Override
        public Iterable<Tuple2<String, TreeMap<Long, Double>>> call(Tuple2<String, String> input)
                throws Exception {

            String fileName = input._1();
            String fileContent = input._2();
            SimpleDateFormat parser = new SimpleDateFormat("YYYYMMdd-HHmmss");
            System.err.println("MATZA = " + fileName);
            int gzIndex = fileName.lastIndexOf(".gz");

            Long timestamp = parser
                    .parse(fileName.substring(gzIndex - 15, gzIndex))
                    .getTime();
            List<String> lines = asList(fileContent.split("\\r?\\n"));
            Map<String, Tuple2<String, TreeMap<Long, Double>>> counts = new HashMap<>();
            for (String line : lines) {
                try {
                    String[] tokens = line.split(" ");
                    if (tokens.length >= 3) {
                        String topic = tokens[1];
                        Double count = parseDouble(tokens[2]);
                        if (counts.containsKey(topic)) {
                            TreeMap<Long, Double> map = counts.get(topic)._2;
                            map.put(timestamp, map.get(timestamp) + count);
                        } else {
                            TreeMap<Long, Double> map = new TreeMap<>();
                            map.put(timestamp, count);
                            counts.put(topic, new Tuple2<>(topic, map));
                        }
                    }
                } catch (Exception e) {
                }
            }

            return counts.values();
        }
    }
}
