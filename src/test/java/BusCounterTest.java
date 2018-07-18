import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class BusCounterTest {

    private static SparkSession sparkSession;

    @BeforeClass public static void init() {
        sparkSession = SparkSession.builder().master("local[*]").appName("CountryFacts")
                .config("spark.sql.parquet.binaryAsString", "true").getOrCreate();
    }

    @Test public void allCountedBuses() {
        Dataset<Row> testDs = ParquetReader.read("/Users/nicoletav/relationDsTest.parquet", sparkSession);
        final int expected = (int) BusCounter.countAll(testDs);
        final int actual = 56;
        assertEquals(expected, actual);
    }

    @After public void tearDown() {
        sparkSession.close();
    }
}