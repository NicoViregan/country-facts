import UDF.IsWheelchairAccessBusUDF;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class BusCounterTest {

    private static SparkSession sparkSession;

    @BeforeClass public static void init() {
        sparkSession = SparkSession.builder().master("local[*]").appName("CountryFacts")
                .config("spark.sql.parquet.binaryAsString", "true").getOrCreate();
    }

    @Test public void allCountedBuses() {
        Dataset<Row> testDs = ParquetReader.read("/Users/nicoletav/relationDsTest.parquet", sparkSession);
        Assert.assertEquals((int) BusCounter.countBuses(testDs), 56);
    }

    @After public void tearDown() {
        sparkSession.close();
    }
}