package classifier;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import scala.Tuple3;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

public class PeakSignalExtractor implements Serializable {

    public static void main(String[] args) throws Exception {
        String inputFilePattern = args[0];
        String outputFilePattern = args[1];
        Double viewsThreshold = Double.parseDouble(args[2]);
        String electionEventsStr = IOUtils.toString(PeakSignalExtractor.class.getResourceAsStream("/election_events"));

        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("PeakSignalExtractor"));

        sc
                .textFile(inputFilePattern)
                .flatMap(new SpikesToJson(electionEventsStr, viewsThreshold))
                .repartition(1)
                .saveAsTextFile(outputFilePattern);

        sc.stop();
    }

    private static final class SpikesToJson implements FlatMapFunction<String, String> {
        private List<Long> eventTimestamps = new ArrayList<>();
        Double viewsThreshold;

        public SpikesToJson(String electionEventsStr, Double viewsThreshold) {
            String[] splits = electionEventsStr.split("\\r?\\n");
            SimpleDateFormat parser = new SimpleDateFormat("yyyy-MM-dd");
            parser.setTimeZone(TimeZone.getTimeZone("CST"));

            for (String eventDateStr : splits) {
                try {
                    eventTimestamps.add(parser.parse(eventDateStr).getTime());
                } catch (Exception e) {}
            }
            this.viewsThreshold = viewsThreshold;
        }

        @Override
        public Iterable<String> call(String line) throws Exception {
            List<String> jsons = new ArrayList<>();
            List<Tuple3<Long, Double, Double>> timeSeries = new ArrayList<>();
            String topic;

            try {
                topic = line.substring(1, line.indexOf(",["));
                String[] numbers = line.replaceAll("[^0-9,.]", "").split(",");
                if (numbers.length < 4) {
                    return jsons;
                }
                for (int i = 1; i < numbers.length; i += 3) {
                    try {
                        Long timeStamp = Long.parseLong(numbers[i]);
                        Double views = Double.parseDouble(numbers[i + 1]);
                        Double signal = Double.parseDouble(numbers[i + 2]);
                        timeSeries.add(new Tuple3(timeStamp, views, signal));
                    } catch (Exception e) {}
                }
            } catch (Exception e) {
                return jsons;
            }


            long msPerHour = 60 * 60 * 1000;
            for (Long timeStamp : eventTimestamps) {
                try {
                    boolean signalFound = false;
                    double totalViews = 0;
                    int count = 0;
                    JSONObject json = new JSONObject();
                    json.put("topic", topic);
                    json.put("event_timestamp", timeStamp);

                    // two days before
                    int[] starts = {-48, -24,  0, 24, 48};
                    int[] ends =   {-24,   0, 24, 48, 72};
                    String[] keys = {"two_days_before", "one_day_before", "on_the_day", "one_day_after", "two_days_after"};

                    for (int i = 0; i < starts.length; i++) {
                        int start = bisectLeft(timeSeries, timeStamp + starts[i] * msPerHour);
                        int end = bisectRight(timeSeries, timeStamp + ends[i] * msPerHour);
                        JSONArray views = new JSONArray();
                        for (int j = start; j < end; j++) {
                            views.put(timeSeries.get(j)._2());
                            totalViews += timeSeries.get(j)._2();
                            count += 1;
                            signalFound = signalFound || timeSeries.get(j)._3() > 0d;
                        }
                        json.put(keys[i], views);
                    }

                    if (signalFound &&  (totalViews / count) >= viewsThreshold) {
                        jsons.add(json.toString());
                    }
                } catch (Exception e) {}
            }
            return jsons;
        }

        private int bisectLeft(List<Tuple3<Long, Double, Double>> timeSeries, Long target) {
            int start = 0;
            int end = timeSeries.size();
            while (start < end) {
                int mid = start + (end - start) / 2;
                if (timeSeries.get(mid)._1() < target)
                    start = mid + 1;
                else
                    end = mid;
            }
            return start;
        }

        private int bisectRight(List<Tuple3<Long, Double, Double>> timeSeries, Long target) {
            int start = 0;
            int end = timeSeries.size();
            while (start < end) {
                int mid = start + (end - start) / 2;
                if (target < timeSeries.get(mid)._1())
                    end = mid;
                else
                    start = mid + 1;
            }
            return start;
        }
    }
}
