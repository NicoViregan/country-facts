import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;


public class NodeWayMerger {


    public static Dataset<Row> explodeNodes(final Dataset<Row> wayDs) {
        return wayDs.select(explode(col("nodes")).as("each_node"), col("id"), col("tags"));
    }

    public static Dataset<Row> addIndexAndIdColumns(final Dataset<Row> explodedWay) {
        final Dataset<Row> result = explodedWay.withColumn("node_id", col("each_node.nodeId"))
                .withColumn("segment_in_way", col("each_node.index"))
                .select(col("node_id"), col("segment_in_way"), col("id"), col("tags"));
        return result;
    }

    public static Dataset<Row> join(final Dataset<Row> newWayDs, final Dataset<Row> nodeDs) {
        return newWayDs.join(nodeDs, newWayDs.col("node_id").equalTo(nodeDs.col("node_id")), "inner");
    }

    public static Dataset<Row> createJoinedDs(final Dataset<Row> nodeDs, final Dataset<Row> wayDs) {
        Dataset<Row> explodedWay = explodeNodes(wayDs);
        Dataset<Row> explodedWayWithIdAndIndexColumns = addIndexAndIdColumns(explodedWay);
        Dataset<Row> renameNodeDs = ColumnsRenamer.rename(nodeDs);
        return NodeWayMerger.join(explodedWayWithIdAndIndexColumns, renameNodeDs);
    }
}
