import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import utilities.RoadTypes;

import static org.junit.Assert.*;


public class CountCrossingsByTypeTest {
    private static SparkSession sparkSession;
    private static CrossingCounter counter;
    private static Dataset<Row> joinedTestDs;

    @BeforeClass public static void init() {
        sparkSession = SparkSession.builder().master("local[*]").appName("CountryFacts")
                .config("spark.sql.parquet.binaryAsString", "true").getOrCreate();
        counter = new CrossingCounter(sparkSession);
        joinedTestDs = ParquetReader.read("/Users/nicoletav/nodeWayJoinedTest.parquet", sparkSession);
    }

    @Test public void countCrossingsOnTrunk() {
        final int expected = (int) counter.countByHighwayType(joinedTestDs, RoadTypes.TRUNK.toString().toLowerCase());
        final int actual = 0;
        assertEquals(expected, actual);
    }
    @Test public void countCrossingsOnMotorway() {
        final int expected = (int) counter.countByHighwayType(joinedTestDs, RoadTypes.MOTORWAY.toString().toLowerCase());
        final int actual = 0;
        assertEquals(expected, actual);
    }
    @Test public void countCrossingsOnResidential() {
        final int expected = (int) counter.countByHighwayType(joinedTestDs, RoadTypes.RESIDENTIAL.toString().toLowerCase());
        final int actual = 6;
        assertEquals(expected, actual);
    }
    @Test public void countCrossingsOnPrimary() {
        final int expected = (int) counter.countByHighwayType(joinedTestDs, RoadTypes.PRIMARY.toString().toLowerCase());
        final int actual = 3;
        assertEquals(expected, actual);
    }
    @Test public void countCrossingsOnSecondary() {
        final int expected = (int) counter.countByHighwayType(joinedTestDs, RoadTypes.SECONDARY.toString().toLowerCase());
        final int actual = 4;
        assertEquals(expected, actual);
    }

    @After public void tearDown() {
        sparkSession.close();
    }
}