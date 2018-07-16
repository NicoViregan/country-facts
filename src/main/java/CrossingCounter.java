import UDF.IsCrossingUDF;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;


public class CrossingCounter {

    public CrossingCounter(SparkSession sparkSession) {
        sparkSession.udf().register("isOnHighway", new IsCrossingUDF(), DataTypes.BooleanType);
    }

    public long countAll(Dataset<Row> nodeDs) {
        final Dataset<Row> crossings = nodeDs.filter(array_contains(col("tags_node.value"), "crossing"));
        return crossings.count();

    }


    public long countCrossings(Dataset<Row> completeDs, String valueType) {
        return completeDs.filter(array_contains(col("tags_node.value"), "crossing"))
                .filter(callUDF("isOnHighway", col("tags"), lit(valueType))).count();
    }


}
