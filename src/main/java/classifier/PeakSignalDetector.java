package classifier;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class PeakSignalDetector implements Serializable {
    /**
    * movingstd: efficient windowed standard deviation of a time series
    * usage: s = movingstd(x,k,windowmode)

    * Movingstd uses filter to compute the standard deviation, using
    * the trick of std = sqrt((sum(x.^2) - n*xbar.^2)/(n-1)).
    * Beware that this formula can suffer from numerical problems for
    * data which is large in magnitude.
    */
    public List<Double> movingStd(List<Double> timeSeries, int K) {
        List<Double> meanSubtracted = new ArrayList<>();
        // Subtract the mean timeSeries = timeSeries - mean(timeSeries)
        double mean = 0;
        for (int i = 0; i < timeSeries.size(); i++) {
            mean += timeSeries.get(i);
        }
        mean /= timeSeries.size();

        for (int i = 0; i < timeSeries.size(); i++) {
            meanSubtracted.add(timeSeries.get(i) - mean);
        }

        int count = 0;
        double sum = 0;
        double sumOfSquares = 0;
        List<Double> std = new ArrayList<>();
        for (int i = 0; i < meanSubtracted.size(); i++) {
            if (count >= K) {
                sum -= meanSubtracted.get(i - K);
                sumOfSquares -= Math.pow(meanSubtracted.get(i - K), 2);
                count -= 1;
            }
            sum += meanSubtracted.get(i);
            sumOfSquares += Math.pow(meanSubtracted.get(i), 2);
            count += 1;
            std.add(Math.sqrt((sumOfSquares - Math.pow(sum, 2) / K)) / (K - 1));
        }
        return std;
    }

    public List<Double> movingMean(List<Double> timeSeries, int K) {
        List<Double> mean = new ArrayList<>();
        int count = 0;
        double sum = 0;
        for (int i = 0; i < timeSeries.size(); i++) {
            if (count >= K) {
                sum -= timeSeries.get(i - K);
            }
            sum += timeSeries.get(i);
            count += 1;
            mean.add(sum / K);
        }
        return mean;
    }

    public List<Double> processTimeSeries(List<Double> timeSeries, int K, double dist) {
        List<Double> movingStd = movingStd(timeSeries, K);
        List<Double> movingMean = movingMean(timeSeries, K);

        double prevStd = movingStd.get(K);
        double prevMean = movingMean.get(K);

        List<Double> signal = new ArrayList<>();
        for (int i = 0; i < K; i++) {
            signal.add(0d);
        }

        for (int i = K; i < timeSeries.size(); i++) {
            if (timeSeries.get(i) > prevMean + dist * prevStd) {
                signal.add(1d);
            } else {
                prevStd = (prevStd + Math.abs(timeSeries.get(i) - prevMean)) / 2;
                prevMean = (prevMean + timeSeries.get(i)) / 2;
                signal.add(0d);
            }
        }
        return signal;
    }
}
