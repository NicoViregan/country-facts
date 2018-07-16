import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;


public class RenameDatasets {

    public static Dataset<Row> renameNodeDs(Dataset<Row> nodeDs) {
        Dataset<Row> renamedDs = nodeDs.withColumnRenamed("id", "id_node").withColumnRenamed("version", "version_node")
                .withColumnRenamed("timestamp", "timestamp_node").withColumnRenamed("changeset", "changeset_node")
                .withColumnRenamed("uid", "uid_node").withColumnRenamed("tags", "tags_node")
                .withColumnRenamed("user_sid", "user_sid_node");

        return renamedDs;
    }


}
