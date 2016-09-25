package downloader;

import java.util.ArrayList;
import java.util.List;

/** Helper methods for the downloader. */
final class DownloadHelper {
    private static final String JANUARY_PAGEVIEWS_EXTENSION =
            "https://dumps.wikimedia.org/other/pageviews/2016/2016-01/pageviews-201601";
    private static final String FEBRUARY_PAGEVIEWS_EXTENSION =
            "https://dumps.wikimedia.org/other/pageviews/2016/2016-02/pageviews-201602";
    private static final String MARCH_PAGEVIEWS_EXTENSION =
            "https://dumps.wikimedia.org/other/pageviews/2016/2016-03/pageviews-201603";
    private static final String APRIL_PAGEVIEWS_EXTENSION =
            "https://dumps.wikimedia.org/other/pageviews/2016/2016-04/pageviews-201604";
    private static final String MAY_PAGEVIEWS_EXTENSION =
            "https://dumps.wikimedia.org/other/pageviews/2016/2016-05/pageviews-201605";
    private static final String JUNE_PAGEVIEWS_EXTENSION =
            "https://dumps.wikimedia.org/other/pageviews/2016/2016-06/pageviews-201606";
    private static final String JULY_PAGEVIEWS_EXTENSION =
            "https://dumps.wikimedia.org/other/pageviews/2016/2016-07/pageviews-201607";
    private static final String AUGUST_PAGEVIEWS_EXTENSION =
            "https://dumps.wikimedia.org/other/pageviews/2016/2016-08/pageviews-201608";
    private static final String SEPTEMBER_PAGEVIEWS_EXTENSION =
            "https://dumps.wikimedia.org/other/pageviews/2016/2016-09/pageviews-201609";
    private static final String ARCHIVE_EXTENSION = "gz";

    /** List of urls where to download the gz archives for January. */
    static List<String> getUrlsForJanuary() {
       return createUrls(1, 31, JANUARY_PAGEVIEWS_EXTENSION);
    }

    /** List of urls where to download the gz archives for February. */
    static List<String> getUrlsForFebruary() {
        return createUrls(1, 29, FEBRUARY_PAGEVIEWS_EXTENSION);
    }

    /** List of urls where to download the gz archives for March. */
    static List<String> getUrlsForMarch() {
        return createUrls(1, 31, MARCH_PAGEVIEWS_EXTENSION);
    }

    /** List of urls where to download the gz archives for April. */
    static List<String> getUrlsForApril() {
        return createUrls(1, 30, APRIL_PAGEVIEWS_EXTENSION);
    }

    /** List of urls where to download the gz archives for May. */
    static List<String> getUrlsForMay() {
        return createUrls(1, 31, MAY_PAGEVIEWS_EXTENSION);
    }

    /** List of urls where to download the gz archives for June. */
    static List<String> getUrlsForJune() {
        return createUrls(1, 30, JUNE_PAGEVIEWS_EXTENSION);
    }

    /** List of urls where to download the gz archives July. */
    static List<String> getUrlsForJuly() {
        return createUrls(1, 31, JULY_PAGEVIEWS_EXTENSION);
    }

    /** List of urls where to download the gz archives for August. */
    static List<String> getUrlsForAugust() {
        return createUrls(1, 31, AUGUST_PAGEVIEWS_EXTENSION);
    }

    /** List of urls where to download the gz archives for September. */
    static List<String> getUrlsForSeptember() {
        return createUrls(1, 30, SEPTEMBER_PAGEVIEWS_EXTENSION);
    }

    /** Returns the name of a gz archive from a download url. */
    static String getFileNameFromUrl(String url) {
        String[] urlParts = url.split("/");
        return urlParts[urlParts.length - 1];
    }

    private static List<String> createUrls(int startDay, int endDay, String monthPageviews) {
        List<String> urls = new ArrayList<>();
        for (int day = startDay; day <= endDay; day++) {
            for (int hour = 0; hour <= 23; hour++) {
                urls.add(String.format("%s%02d-%02d0000.%s", monthPageviews, day, hour, ARCHIVE_EXTENSION));
            }
        }

        return urls;
    }
}
