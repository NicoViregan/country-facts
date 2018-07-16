import UDF.IsCrossingPrimaryUDF;
import UDF.IsCrossingResidentialUDF;
import UDF.IsCrossingSecondary;
import UDF.IsWheelchairAccessBusUDF;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.array_contains;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;


public class CrossingCounter {

    public static long countAll(Dataset<Row> nodeDs) {
        final Dataset<Row> crossings = nodeDs.filter(array_contains(col("tags_node.value"), "crossing"));
        return crossings.count();

    }

    public static long countResidential(SparkSession sparkSession, Dataset<Row> completeDs) {
        sparkSession.udf().register("isOnResidential", new IsCrossingResidentialUDF(), DataTypes.BooleanType);

        return completeDs.filter(array_contains(col("tags_node.value"), "crossing"))
                .filter(callUDF("isOnResidential", col("tags"))).count();


    }

    public static long countPrimary(SparkSession sparkSession, Dataset<Row> completeDs) {
        sparkSession.udf().register("isOnPrimary", new IsCrossingPrimaryUDF(), DataTypes.BooleanType);

        return completeDs.filter(array_contains(col("tags_node.value"), "crossing"))
                .filter(callUDF("isOnPrimary", col("tags"))).count();


    }

    public static long countSecondary(SparkSession sparkSession, Dataset<Row> completeDs) {
        sparkSession.udf().register("isOnSecondary", new IsCrossingSecondary(), DataTypes.BooleanType);

        return completeDs.filter(array_contains(col("tags_node.value"), "crossing"))
                .filter(callUDF("isOnSecondary", col("tags"))).count();


    }
}
