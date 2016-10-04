package classifier;

import java.util.*;

public class ReferenceTrends {
    private final List<List<Double>> trends = new ArrayList<>();
    private final List<List<Double>> nonTrends = new ArrayList<>();
    private static final double EPSILON = 0.00001;
    private final int referenceLength;
    private final int basefileOffset;
    private final int nSmooth;
    private final double alpha;

    public ReferenceTrends(
            Optional<Integer> referenceLength,
            Optional<Integer> baselineOffset,
            Optional<Integer> nSmooth,
            Optional<Double> alpha
    ) {
        this.referenceLength = referenceLength.isPresent() ? referenceLength.get() : 210;
        this.basefileOffset = baselineOffset.isPresent() ? baselineOffset.get() : 40;
        this.nSmooth = nSmooth.isPresent() ? nSmooth.get() : 80;
        this.alpha = alpha.isPresent() ? alpha.get() : 1.2;
    }

    public void addReferenceTrend(List<Double> series, boolean trending) {
        transformTrend(series);
        sizing(series);
        if (trending) {
            trends.add(series);
        }
        else {
            nonTrends.add(series);
        }
    }

    public void transformTrend(List<Double> series) {
        addOne(series);
        unitNormalization(series);
        logarithmicScaling(series);
        smoothing(series);
    }

    /**
     * Add a count of 1 to every count in the series
     * @param series
     */
    public void addOne(List<Double> series) {
        for (int i = 0; i < series.size(); i++) {
            series.set(i, series.get(i) + 1);
        }
    }

    /**
     * Do unit normalization based on "reference_length" number of bins at the
     * end of the series
     * @param series
     */
    public void unitNormalization(List<Double> series) {
        double sum = 0;
        for (int i = series.size() - basefileOffset - referenceLength; i < series.size() - basefileOffset; i++) {
            sum += series.get(i);
        }
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
            while (dq.size() > nSmooth) {
                dqSum -= dq.removeFirst();
            }
        }
    }

    public void logarithmicScaling(List<Double> series) {
        for (int i = 0; i < series.size(); i++) {
            series.set(i, Math.log10(series.get(i) < 0 ? EPSILON : series.get(i)));
        }
    }

    public void sizing(List<Double> series) {
        while (series.size() > referenceLength) {
            series.remove(0);
        }
    }
}
