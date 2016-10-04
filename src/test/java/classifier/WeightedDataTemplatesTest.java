package classifier;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static java.lang.Double.parseDouble;
import static java.lang.Integer.parseInt;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Data for the test has been taken from the examples/ folder in the Gnip-Trend-Detection repo:
 * https://github.com/jeffakolb/Gnip-Trend-Detection/tree/92d71c3460db1482dc5bb0e640cea2d4d725e5ec/example
 */
@RunWith(JUnit4.class)
public class WeightedDataTemplatesTest {
    private static final String CONFIG_FILE = "/config.properties";
    private static final String SERIES_LENGTH = "seriesLength";
    private static final String REFERENCE_LENGTH = "referenceLength";
    private static final String LAMBDA = "lambda";
    private static final String BASELINE_OFFSET = "baselineOffset";
    private static final String N_SMOOTH = "nSmooth";
    private static final String ALPHA = "alpha";
    private static final double EPSILON = 0.001;
    private static final double[] INPUT =
            new double[] {76, 91, 69, 76, 39, 36, 59, 58, 58, 66, 70, 34, 74, 180, 93, 47, 37, 26, 23, 67, 62, 270, 460,
                    160, 91, 130, 110, 110, 310, 220, 330, 170, 240, 160, 90, 250, 120, 190, 170, 140, 160, 110, 97,
                    180, 120, 110, 240, 180, 110, 140, 83, 49, 75, 66, 150, 170, 200, 160, 160, 200, 77, 190, 130, 210,
                    170, 190, 190, 200, 160, 210, 240, 170, 82, 140, 89, 57, 77, 96, 130, 180, 130, 95, 80, 170, 84, 80,
                    55, 36, 28, 40, 71, 67, 90, 170, 140, 86, 94, 110, 93, 87, 32, 27, 91, 120, 74, 170, 170, 91, 71,
                    56, 120, 79, 68, 100, 110, 180, 150, 180, 280, 180, 200, 150, 120, 85, 77, 95, 190, 190, 240, 200,
                    130, 110, 110, 120, 100, 66, 53, 41, 63, 120, 140, 140, 160, 73, 99, 170, 63, 60, 89, 120, 320, 180,
                    140, 200, 160, 110, 170, 150, 100, 58, 44, 48, 84, 140, 100, 140, 77, 53, 110, 100, 120, 75, 62, 65,
                    260, 160, 62, 83, 120, 61, 110, 150, 64, 67, 57, 66, 120, 96, 110, 180, 120, 94, 100, 79, 75, 43,
                    65, 75, 110, 150, 180, 120, 140, 94, 110, 97, 86, 58, 52, 43, 230, 320, 99, 130, 470, 190, 140, 170,
                    75, 56, 51, 51, 89, 110, 84, 82, 100, 85, 140, 130, 97, 14, 28, 35, 54, 130, 150, 110, 120, 45, 240,
                    65, 75, 32, 16, 25, 32, 52, 80, 48, 45, 55, 160, 120, 41, 37, 16, 11, 36, 33, 64, 39, 44, 52, 280,
                    84, 24, 13, 17, 130, 110, 140, 150, 120, 140, 120, 240, 120, 57, 48, 78, 57, 87, 100, 130, 93, 110,
                    49, 260, 290, 190, 110, 65, 120, 160, 180, 140, 180, 140, 130, 170, 93, 57, 59, 55, 38, 72, 120, 77,
                    76, 76, 67, 86, 92, 95, 140, 290, 400, 330, 370, 480, 1200, 1400, 510, 290, 750, 480, 440, 420, 340,
                    440, 880, 420, 290, 190, 150, 820, 220, 160, 130, 160, 510, 320, 430, 160, 190, 90, 86, 1000, 310,
                    190, 210, 88, 450, 260, 230, 130, 340, 180, 180, 1100, 430, 160, 340, 280, 350, 570, 390, 48};
    private static final double[] EXPECTED =
            new double[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0.46, 0.51, 0.52, 0.52, 0.53, 0.53, 0.51,
                    0.51, 0.51, 0.5, 0.48, 0.46, 0.45, 0.45, 0.46, 0.46, 0.46, 0.46, 0.45, 0.44, 0.44, 0.44, 0.44, 0.44,
                    0.44, 0.47, 0.48, 0.47, 0.45, 0.46, 0.45, 0.45, 0.46, 0.46, 0.45, 0.44, 0.43, 0.44, 0.44, 0.45,
                    0.47, 0.47, 0.46, 0.45, 0.45, 0.44, 0.43, 0.43, 0.43, 0.44, 0.46, 0.48, 0.48, 0.48, 0.47, 0.47,
                    0.46, 0.46, 0.45, 0.45, 0.44, 0.47, 0.52, 0.51, 0.5, 0.55, 0.56, 0.55, 0.55, 0.53, 0.5, 0.49, 0.47,
                    0.47, 0.47, 0.47, 0.47, 0.47, 0.47, 0.48, 0.5, 0.49, 0.48, 0.46, 0.46, 0.45, 0.46, 0.48, 0.48, 0.48,
                    0.47, 0.49, 0.48, 0.47, 0.45, 0.44, 0.44, 0.43, 0.43, 0.42, 0.42, 0.41, 0.41, 0.43, 0.44, 0.43,
                    0.42, 0.41, 0.4, 0.4, 0.39, 0.39, 0.38, 0.38, 0.38, 0.41, 0.41, 0.4, 0.39, 0.38, 0.39, 0.4, 0.42,
                    0.43, 0.44, 0.44, 0.44, 0.47, 0.47, 0.45, 0.43, 0.42, 0.41, 0.41, 0.42, 0.43, 0.43, 0.44, 0.43,
                    0.46, 0.5, 0.52, 0.51, 0.48, 0.47, 0.49, 0.51, 0.51, 0.52, 0.52, 0.52, 0.53, 0.52, 0.5, 0.49, 0.47,
                    0.46, 0.46, 0.47, 0.47, 0.47, 0.46, 0.45, 0.46, 0.46, 0.47, 0.48, 0.52, 0.58, 0.61, 0.64, 0.68,
                    0.76, 0.82, 0.82, 0.81, 0.86, 0.88, 0.89, 0.9, 0.89, 0.91, 0.98, 0.98, 0.97, 0.93, 0.88, 0.96, 0.94,
                    0.9, 0.86, 0.83, 0.9, 0.93, 0.98, 0.94, 0.92, 0.86, 0.81, 0.91, 0.94, 0.93, 0.92, 0.86, 0.91, 0.92,
                    0.93, 0.88, 0.89, 0.86, 0.82, 0.91, 0.89, 0.79, 0.78, 0.8, 0.8, 0.84, 0.86, 0.78};

    @Test
    public void classify() throws IOException, ClassNotFoundException {
        InputStream input = getClass().getResourceAsStream(CONFIG_FILE);
        assertNotNull(input);

        Properties properties = new Properties();
        properties.load(input);
        input.close();

        WeightedDataTemplates template =
                new WeightedDataTemplates(
                        parseInt(properties.getProperty(SERIES_LENGTH)),
                        parseInt(properties.getProperty(REFERENCE_LENGTH)),
                        parseDouble(properties.getProperty(LAMBDA)),
                        new ReferenceTrends(
                                parseInt(properties.getProperty(REFERENCE_LENGTH)),
                                parseInt(properties.getProperty(BASELINE_OFFSET)),
                                parseInt(properties.getProperty(N_SMOOTH)),
                                parseDouble(properties.getProperty(ALPHA))));

        List<Double> results = new ArrayList<>();
        for (double d : INPUT) {
            template.update(d);
            results.add(template.getResult());
        }

        assertEquals(results.size(), EXPECTED.length);

        for (int i = 0; i < results.size(); i++) {
            assertEquals(results.get(i), EXPECTED[i], EPSILON);
        }
    }
}
