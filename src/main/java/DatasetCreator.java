import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.col;


public class DatasetCreator {

    public static Dataset<Row> explodeNodes(Dataset<Row> nodeDs) {
        return nodeDs
                .select(org.apache.spark.sql.functions.explode(col("nodes")).as("each_node"), col("id"), col("version"),
                        col("timestamp"), col("changeset"), col("user_sid"), col("tags"));
    }

    public static Dataset<Row> addIndexAndIdColumns(Dataset<Row> explodedWay) {
        return explodedWay.withColumn("node_id", col("each_node.nodeId"))
                .withColumn("segment_in_way", col("each_node.index"));
    }

    public static Dataset<Row> join(Dataset<Row> newWayDs, Dataset<Row> nodeDs) {
        Dataset<Row> nodeDsWithChangedColumnName = nodeDs.withColumnRenamed("id", "node_id");
        return newWayDs.join(nodeDsWithChangedColumnName, "node_id");
    }
}
