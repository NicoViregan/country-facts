import udf.IsCrossingUDF;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import static org.apache.spark.sql.functions.*;


public class CrossingCounter {

    public CrossingCounter(final SparkSession sparkSession) {
        sparkSession.udf().register("isOnHighway", new IsCrossingUDF(), DataTypes.BooleanType);
    }

    public long countAll(final Dataset<Row> completeDs) {
        final Dataset<Row> crossings = completeDs.filter(array_contains(col("node_tags.value"), "crossing"));
        return crossings.count();
    }

    public long countByHighwayType(final Dataset<Row> completeDs, final String valueType) {
        return completeDs.filter(array_contains(col("node_tags.value"), "crossing"))
                .filter(callUDF("isOnHighway", col("tags"), lit(valueType))).count();
    }
}



