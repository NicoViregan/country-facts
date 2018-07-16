import UDF.IsWheelchairAccessBusUDF;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.junit.BeforeClass;
import org.junit.Test;


public class IsWheelchairAccessBusUDFTest {
    private SparkSession sparkSession;

    @BeforeClass
    public void init(){
        SparkSession sparkSession = SparkSession.builder().master("local[*]").appName("CountryFacts")
                .config("spark.sql.parquet.binaryAsString", "true").getOrCreate();
    }

    @Test
    public void isWheelchairWhenTagYes(){
        sparkSession.udf().register("hasWheelchair", new IsWheelchairAccessBusUDF(), DataTypes.BooleanType);

    }


}