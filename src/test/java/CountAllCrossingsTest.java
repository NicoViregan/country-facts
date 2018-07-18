import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;


public class CountAllCrossingsTest {

    private static SparkSession sparkSession;
    private static CrossingCounter counter;

    @BeforeClass public static void init() {
        sparkSession = SparkSession.builder().master("local[*]").appName("CountryFacts")
                .config("spark.sql.parquet.binaryAsString", "true").getOrCreate();
        counter = new CrossingCounter(sparkSession);
    }

    @Test public void countAllCrossings() {
        Dataset<Row> testDs = ParquetReader.read("/Users/nicoletav/nodeWayJoinedTest.parquet", sparkSession);
        final int expected = (int) counter.countAll(testDs);
        final int actual = 30;
        assertEquals(expected, actual);
    }

    @After public void tearDown() {
        sparkSession.close();
    }

}