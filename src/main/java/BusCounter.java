import UDF.IsWheelchairAccessBusUDF;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.array_contains;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;


public class BusCounter {

    public static long countAll(final Dataset<Row> relationDs) {
        final Dataset<Row> busRoutes = relationDs.filter(array_contains(col("tags.key"), "route"))
                .filter(array_contains(col("tags.value"), "bus"));
        return busRoutes.count();
    }

    public static long countWithWheelchair(final SparkSession sparkSession, final Dataset<Row> relationDs) {
        sparkSession.udf().register("hasBusWheelchair", new IsWheelchairAccessBusUDF(), DataTypes.BooleanType);
        final Dataset<Row> busRelations = relationDs.filter(callUDF("hasBusWheelchair", col("tags")));

        return busRelations.count();
    }
}
