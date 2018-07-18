import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;


public class ColumnsRenamer {

    public static Dataset<Row> rename(final Dataset<Row> nodeDs) {
        Dataset<Row> renamedDs =
                nodeDs.withColumnRenamed("id", "node_id").withColumnRenamed("timestamp", "node_timestamp")
                        .withColumnRenamed("tags", "node_tags").select("node_id", "node_timestamp", "node_tags");

        return renamedDs;
    }
}
