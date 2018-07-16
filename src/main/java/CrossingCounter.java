import UDF.IsCrossingUDF;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;


public class CrossingCounter {

    public static long countAll(Dataset<Row> nodeDs) {
        final Dataset<Row> crossings = nodeDs.filter(array_contains(col("tags_node.value"), "crossing"));
        return crossings.count();

    }

    public static long countCrossings(SparkSession sparkSession, Dataset<Row> completeDs, String valueType) {
        sparkSession.udf().register("isOnResidential", new IsCrossingUDF(), DataTypes.BooleanType);
        return completeDs.filter(array_contains(col("tags_node.value"), "crossing"))
                .filter(callUDF("isOnResidential", col("tags"), lit(valueType))).count();
    }


}
