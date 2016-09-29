package downloader;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.io.FileOutputStream;
import java.io.Serializable;
import java.net.URL;
import java.nio.channels.ReadableByteChannel;

import static downloader.DownloadHelper.getFileNameFromUrl;
import static downloader.DownloadHelper.getUrlsForFebruary;
import static downloader.DownloadHelper.getUrlsForSeptember;
import static java.nio.channels.Channels.newChannel;

/**
 * Tool for downloading the Wikipedia pageviews dumps from https://dumps.wikimedia.org/other/pageviews/ for the months
 * between January and June 2016.
 * Please note that this class has to be public and serializable in order to be submitted as a Spark job.
 */
public final class Dowloader implements Serializable {

    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("Downloader"));

        sc
                .parallelize(getUrlsForSeptember(), 3)
                .foreach((VoidFunction<String>) url -> {
                    URL downloadUrl= new URL(url);
                    ReadableByteChannel rbc = newChannel(downloadUrl.openStream());
                    FileOutputStream fos = new FileOutputStream(
                            "/output/path" + getFileNameFromUrl(url));
                    fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);

                    System.out.println("[downloader] Finished downloading " + url);
                });
    }
}
