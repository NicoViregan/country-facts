import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.array_contains;
import static org.apache.spark.sql.functions.col;


public class TestDataSetCreator {

    ParquetWriter w = new ParquetWriter();

    private Dataset<Row> getMinRelationRowsNeeded(final SparkSession sparkSession, final Dataset<Row> relationDs) {
        int indexDs = 1;
        while (BusCounter.countWithWheelchair(sparkSession, relationDs.limit(indexDs)) == 0) {
            ++indexDs;
        }
        return relationDs.limit(indexDs);

    }

    public void createRelationParquet(final SparkSession session, final Dataset<Row> relationDs) {
        w.write("relationDsTest.parquet", getMinRelationRowsNeeded(session, relationDs));
    }

    public void createNodeParquet(final Dataset<Row> nodeDs) {
        Dataset<Row>  crossingNodesDs = nodeDs.filter(array_contains(col("tags.value"), "crossing"));
        Dataset<Row> chosenJoinedDs = crossingNodesDs.limit(30);
        w.write("nodeDsTest.parquet", chosenJoinedDs);
    }

    public  void createNodeWayParquet( final Dataset<Row> nodeDs, final Dataset<Row> wayDs){
        Dataset<Row> nodeWayJoined = NodeWayMerger.createJoinedDs(nodeDs, wayDs);
        Dataset<Row>  filteredJoinedDs = nodeWayJoined.filter(array_contains(col("node_tags.value"), "crossing"));
        Dataset<Row> chosenJoinedDs = filteredJoinedDs.limit(30);
        chosenJoinedDs.show(3, false);
        w.write("nodeWayJoinedTest.parquet", chosenJoinedDs);
    }
}
