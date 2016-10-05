package classifier;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.Math.*;

/**
 * This is a rough Java translation of the python code available at:
 * https://github.com/jeffakolb/Gnip-Trend-Detection/blob/master/gnip_trend_detection/models.py
 *
 * The approach has been inspired by Stanislav Nikolov's Msc. thesis:
 * Thesis: https://dspace.mit.edu/bitstream/handle/1721.1/85399/870304955-MIT.pdf?sequence=2
 * Blog post: https://snikolov.wordpress.com/2012/11/14/early-detection-of-twitter-trends/
 */
public final class WeightedDataTemplates implements Serializable {
    private static final double EPSILON = 0.001;

    private final int seriesLength;
    private final int referenceLength;
    private final double lambda;
    private final ReferenceTrends referenceTrends;
    private final List<Double> totalSeries = new LinkedList<>();
    private double totalSeriesSum = 0;
    private double trendWeight;
    private double nonTrendWeight;

    public WeightedDataTemplates(
            int seriesLength,
            int referenceLength,
            double lambda,
            ReferenceTrends referenceTrends) {
        this.seriesLength = seriesLength;
        this.referenceLength = referenceLength;
        this.lambda = lambda;
        this.trendWeight = -1;
        this.nonTrendWeight = -1;
        this.referenceTrends = referenceTrends;
    }

    /** Calculate trend weights for time series based on latest data. */
    public void update(double count) {
        totalSeries.add(count);
        totalSeriesSum += count;
        while (totalSeries.size() > max(referenceLength, seriesLength)) {
            totalSeries.remove(0);
        }

        // Exit early until totalSeries is long enough.
        if (totalSeries.size() < referenceLength || totalSeriesSum == 0) {
            trendWeight = 0;
            nonTrendWeight = 0;
            return;
        }

        // Transform a reference-sized subseries.
        List<Double> transformedSeries = referenceTrends.transformInput(totalSeries);
        // Get correctly sized test series.
        List<Double> testSeries =
                transformedSeries.subList(transformedSeries.size() - seriesLength, transformedSeries.size());

        trendWeight = 0;
        for (List<Double> referenceSeries : referenceTrends.getTrends()) {
            double weight = getWeight(referenceSeries, testSeries);
            trendWeight += weight;
        }

        nonTrendWeight = 0;
        for (List<Double> nonReferenceSeries : referenceTrends.getNonTrends()) {
            double weight = getWeight(nonReferenceSeries, testSeries);
            nonTrendWeight += weight;
        }
    }

    /** Return result or figure-of-merit (ratio of weights, in this case) defined by the mode of operation. */
    public double getResult() {
        if (trendWeight == -1 || nonTrendWeight == -1) {
            return -1;
        }

        if (nonTrendWeight == 0) {
            nonTrendWeight = EPSILON;
        }

        return trendWeight / nonTrendWeight;
    }

    /**
     * Get the minimum distance between the series and all testSeries-length subset of reference_series.
     * Exponentiate it and return the weight. */
    private double getWeight(List<Double> referenceSeries, List<Double> testSeries) {
        double minDistance = Double.POSITIVE_INFINITY;
        for (int i = 0; i <= referenceSeries.size() - seriesLength; i++) {
            List<Double> subSeries = referenceSeries.subList(i, i + seriesLength);
            double d = euclideanDistance(subSeries, testSeries);
            if (d < minDistance) {
                minDistance = d;
            }
        }
        return exp(-minDistance * lambda);
    }

    /** Euclidean distance between two vectors (provided as lists). */
    private double euclideanDistance(List<Double> a, List<Double> b) {
        checkState(a.size() == b.size(), "a and b must have the same length");

        double sum = 0;
        for (int i = 0; i < a.size(); i++) {
            sum += abs(a.get(i) - b.get(i));
        }

        return sum;
    }
}
