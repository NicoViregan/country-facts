import UDF.IsWheelchairAccessBusUDF;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class IsWheelchairAccessBusUDFTest {

    private static SparkSession sparkSession;

    @BeforeClass public static void init() {
        sparkSession = SparkSession.builder().master("local[*]").appName("CountryFacts")
                .config("spark.sql.parquet.binaryAsString", "true").getOrCreate();
    }

    @Test public void isWheelchairWhenTagYes() {
        sparkSession.udf().register("hasWheelchair", new IsWheelchairAccessBusUDF(), DataTypes.BooleanType);
        Dataset<Row> testDs = ParquetReader.read("/Users/nicoletav/relationDsTest.parquet", sparkSession);
        final int  expected= (int) BusCounter.countWithWheelchair(sparkSession, testDs);
        final int  actual= 1;
        assertEquals(expected, actual);
    }

    @After public void tearDown() {
        sparkSession.close();
    }
}