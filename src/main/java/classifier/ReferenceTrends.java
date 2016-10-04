package classifier;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

import static java.lang.Math.max;

public class ReferenceTrends {
    public static final String TRENDS = "/trends.ser";
    public static final String NON_TRENDS = "/non_trends.ser";

    private final List<List<Double>> trends;
    private final List<List<Double>> nonTrends;
    private static final double EPSILON = 0.00001;
    private final int referenceLength;
    private final int basefileOffset;
    private final int nSmooth;
    private final double alpha;

    public ReferenceTrends(
            int referenceLength,
            int baselineOffset,
            int nSmooth,
            double alpha) throws IOException, ClassNotFoundException {
        this.referenceLength = referenceLength;
        this.basefileOffset = baselineOffset;
        this.nSmooth = nSmooth;
        this.alpha = alpha;

        this.trends = readSerializedData(TRENDS);
        this.nonTrends = readSerializedData(NON_TRENDS);
    }

    public List<List<Double>> getTrends() {
        return trends;
    }

    public List<List<Double>> getNonTrends() {
        return nonTrends;
    }

    public void addReferenceTrend(List<Double> series, boolean trending) {
        List<Double> transformedSeries = transformInput(series);
        sizing(transformedSeries);
        if (trending) {
            trends.add(transformedSeries);
        }
        else {
            nonTrends.add(transformedSeries);
        }
    }

    public List<Double> transformInput(List<Double> series) {
        List<Double> seriesCopy = new ArrayList(series);

        addOne(seriesCopy);
        unitNormalization(seriesCopy);
        logarithmicScaling(seriesCopy);
        smoothing(seriesCopy);

        return seriesCopy;
    }

    /** Add a count of 1 to every count in the series. */
    public void addOne(List<Double> series) {
        for (int i = 0; i < series.size(); i++) {
            series.set(i, series.get(i) + 1);
        }
    }

    /**
     * Do unit normalization based on "reference_length" number of bins at the
     * end of the series
     */
    public void unitNormalization(List<Double> series) {
        double sum = 0;
        for (int i = max(series.size() - basefileOffset - referenceLength, 0); i < series.size() - basefileOffset; i++) {
            sum += series.get(i);
        }
        sum /= referenceLength;
        if (sum == 0) {
            sum = EPSILON;
        }
        for (int i = 0; i < series.size(); i++) {
            series.set(i, series.get(i) / sum);
        }
    }

    public void spikeNormalization(List<Double> series) {
        double prev = 0;
        for (int i = 0; i < series.size(); i++) {
            Double tmp = series.get(i);
            series.set(i, Math.pow(Math.abs(series.get(i) - prev), alpha));
            prev = tmp;
        }
    }

    public void smoothing(List<Double> series) {
        Deque<Double> dq = new LinkedList<>();
        double dqSum = 0;
        for (int i = 0; i < series.size(); i++) {
            dq.addLast(series.get(i));
            dqSum += series.get(i);
            series.set(i, dqSum / dq.size());
            if (dq.size() >= nSmooth) {
                dqSum -= dq.removeFirst();
            }
        }
    }

    public void logarithmicScaling(List<Double> series) {
        for (int i = 0; i < series.size(); i++) {
            series.set(i, Math.log10(series.get(i) <= 0 ? EPSILON : series.get(i)));
        }
    }

    public void sizing(List<Double> series) {
        while (series.size() > referenceLength) {
            series.remove(0);
        }
    }

    private List<List<Double>> readSerializedData(String filename) throws IOException, ClassNotFoundException {
        ObjectInputStream oos = new ObjectInputStream(getClass().getResourceAsStream(filename));
        List<List<Double>> data = (List<List<Double>>) oos.readObject();
        oos.close();

        return data;
    }
}
