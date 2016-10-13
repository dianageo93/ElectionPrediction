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
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import scala.Tuple3;

import java.io.InputStream;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.*;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.Double.parseDouble;
import static java.lang.Integer.parseInt;

public final class TrendDetector implements Serializable {
    private static final String CONFIG_FILE_PATH = "/config.properties";
    private static final String SERIES_LENGTH = "seriesLength";
    private static final String REFERENCE_LENGTH = "referenceLength";
    private static final String LAMBDA = "lambda";
    private static final String BASELINE_OFFSET = "baselineOffset";
    private static final String N_SMOOTH = "nSmooth";
    private static final String ALPHA = "alpha";

    private static final String WIN_LENGTH = "winLength";
    private static final String DIFF = "diff";
    private static final String INFLUENCE = "influence";

    public static void main(String[] args) throws Exception {
        String inputFilePattern = args[0];
        String outputFilePattern = args[1];
        final List<String> keyWords = new ArrayList<>(Arrays.asList(args).subList(2, args.length));

        InputStream inputStream = TrendDetector.class.getResourceAsStream(CONFIG_FILE_PATH);
        checkState(inputStream != null, "Could not find config file in class resources.");
        final Properties properties = new Properties();
        properties.load(inputStream);

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
                                try {
                                    return lines.hasNext();
                                } catch (Exception e) {
                                    return false;
                                }
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
//                .filter(new FilterTopics(keyWords))
                .mapToPair(new TopicToTimeseries())
                .reduceByKey(new ReduceByTopic())
//                .mapValues(new ComputeEta(properties))
                .mapValues(new ComputeSignal(properties))
                .saveAsTextFile(outputFilePattern, org.apache.hadoop.io.compress.GzipCodec.class);
//                .saveAsObjectFile(outputFilePattern);
        sc.stop();
    }

    private static final class FilterTopics implements Function<Tuple2<String, String>, Boolean> {
        private final List<String> keyWords;

        public FilterTopics(List<String> keyWords) {
            this.keyWords = keyWords;
        }

        @Override
        public Boolean call(Tuple2<String, String> tup) throws Exception {
            String line = tup._2.toLowerCase();
            for (String keyWord : keyWords) {
                if (line.contains(keyWord)) {
                    return true;
                }
            }

            return false;
        }
    }

    private static final class ComputeSignal implements
            Function<List<Tuple2<Long,Double>>, List<Tuple3<Long, Double, Double>>> {
        private final int winLength;
        private final double diff;
        private final double influence;

        public ComputeSignal(Properties properties) throws Exception {
            winLength = parseInt(properties.getProperty(WIN_LENGTH));
            diff = parseDouble(properties.getProperty(DIFF));
            influence = parseDouble(properties.getProperty(INFLUENCE));
        }

        @Override
        public List<Tuple3<Long, Double, Double>> call(List<Tuple2<Long, Double>> input) throws Exception {
            List<Tuple3<Long, Double, Double>> result = new ArrayList<>();
            if (input.size() <= winLength) {
                return result;
            }
            PeakSignalDetector peakSignalDetector = new PeakSignalDetector();

            List<Double> views = new ArrayList<>();
            for (Tuple2<Long, Double> it : input) {
                views.add(it._2);
            }
            List<Double> signal = peakSignalDetector.processTimeSeries(views, winLength, diff, influence);

            for (int i = 0; i < input.size(); i++) {
                result.add(new Tuple3<>(input.get(i)._1, input.get(i)._2, signal.get(i)));
            }

            return result;
        }
    }

    private static final class ComputeEta implements
            Function<List<Tuple2<Long,Double>>, List<Tuple3<Long, Double, Double>>> {
        private final WeightedDataTemplates dataTemplate;

        public ComputeEta(Properties properties) throws Exception {
            this.dataTemplate =
                    new WeightedDataTemplates(
                            parseInt(properties.getProperty(SERIES_LENGTH)),
                            parseInt(properties.getProperty(REFERENCE_LENGTH)),
                            parseDouble(properties.getProperty(LAMBDA)),
                            new ReferenceTrends(
                                parseInt(properties.getProperty(REFERENCE_LENGTH)),
                                parseInt(properties.getProperty(BASELINE_OFFSET)),
                                parseInt(properties.getProperty(N_SMOOTH)),
                                parseDouble(properties.getProperty(ALPHA))));
        }

        @Override
        public List<Tuple3<Long, Double, Double>> call(List<Tuple2<Long, Double>> input) throws Exception {
            List<Tuple3<Long, Double, Double>> result = new ArrayList<>();
            dataTemplate.getTotalSeries().clear();
            for (Tuple2<Long, Double> it : input) {
                dataTemplate.update(it._2);
                result.add(new Tuple3<>(it._1, it._2, dataTemplate.getResult()));
            }

            return result;
        }
    }

    private static final class ReduceByTopic implements
            Function2<List<Tuple2<Long, Double>>, List<Tuple2<Long, Double>>, List<Tuple2<Long, Double>>> {

        @Override
        public List<Tuple2<Long, Double>> call(List<Tuple2<Long, Double>> list1, List<Tuple2<Long, Double>> list2)
                throws Exception {
            int idx1 = 0;
            int idx2 = 0;
            List<Tuple2<Long, Double>> result = new ArrayList<>();
            while (idx1 < list1.size() && idx2 < list2.size()) {
                Tuple2<Long, Double> tup1 = list1.get(idx1);
                Tuple2<Long, Double> tup2 = list2.get(idx2);
                if (tup1._1.equals(tup2._1)) {
                    result.add(new Tuple2(tup1._1, tup1._2 + tup2._2));
                    idx1 += 1;
                    idx2 += 1;
                } else if (tup1._1.compareTo(tup2._1) < 0) {
                    result.add(tup1);
                    idx1 += 1;
                } else {
                    result.add(tup2);
                    idx2 += 1;
                }
            }
            while (idx1 < list1.size()) {
                result.add(list1.get(idx1));
                idx1 += 1;
            }

            while (idx2 < list2.size()) {
                result.add(list2.get(idx2));
                idx2 += 1;
            }
            return result;
        }
    }

    private static final class TopicToTimeseries implements
            PairFunction<Tuple2<String,String>, String, List<Tuple2<Long, Double>>> {

        @Override
        public Tuple2<String, List<Tuple2<Long, Double>>> call(Tuple2<String, String> input) throws Exception {

            String fileName = input._1();
            String line = input._2();
            SimpleDateFormat parser = new SimpleDateFormat("yyyyMMdd-HHmmss");
            int gzIndex = fileName.lastIndexOf(".gz");

            Long timestamp = parser
                    .parse(fileName.substring(gzIndex - 15, gzIndex))
                    .getTime();

            try {
                String[] tokens = line.split(" ");
                if (tokens.length >= 3 && (tokens[0].startsWith("en") || tokens[0].startsWith("es"))) {
                    // English and Spanish topics
                    String topic = tokens[1];
                    Double count = parseDouble(tokens[2]);
                    return new Tuple2<>(topic.toLowerCase(), Arrays.asList(new Tuple2<>(timestamp, count)));
                }
            } catch (Exception e) {}

            return new Tuple2<>("", Arrays.asList(new Tuple2<>(0L, 0d)));
        }
    }
}
