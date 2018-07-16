import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.col;


public class DatasetCreator {


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
        Dataset<Row> explodedWay = DatasetCreator.explodeNodes(wayDs);
        Dataset<Row> explodedWayWithIdAndIndexColumns = DatasetCreator.addIndexAndIdColumns(explodedWay);
        Dataset<Row> renameNodeDs = RenameDatasets.renameNodeDs(nodeDs);
        return DatasetCreator.join(explodedWayWithIdAndIndexColumns, renameNodeDs);
    }
}
