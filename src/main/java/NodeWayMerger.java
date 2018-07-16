import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.col;


public class NodeWayMerger {


    public static Dataset<Row> explodeNodes(Dataset<Row> wayDs) {
        return wayDs
                .select(org.apache.spark.sql.functions.explode(col("nodes")).as("each_node"), col("id"), col("version"),
                        col("timestamp"), col("changeset"), col("user_sid"), col("tags"));
    }

    public static Dataset<Row> addIndexAndIdColumns(Dataset<Row> explodedWay) {
        return explodedWay.withColumn("id_node", col("each_node.nodeId"))
                .withColumn("segment_in_way", col("each_node.index"));
    }

    public static Dataset<Row> join(Dataset<Row> newWayDs, Dataset<Row> nodeDs) {

        return newWayDs.join(nodeDs, "id_node");
    }

    public static Dataset<Row> createJoinedDs(Dataset<Row> nodeDs, Dataset<Row> wayDs) {
        Dataset<Row> explodedWay = NodeWayMerger.explodeNodes(wayDs);
        Dataset<Row> explodedWayWithIdAndIndexColumns = NodeWayMerger.addIndexAndIdColumns(explodedWay);
        Dataset<Row> renameNodeDs = RenameDatasets.renameNodeDs(nodeDs);
        return NodeWayMerger.join(explodedWayWithIdAndIndexColumns, renameNodeDs);
    }
}
