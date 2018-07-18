import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class TestDatasetCreator {

    public Dataset<Row> getMinRelationRowsNeeded(final SparkSession sparkSession, final Dataset<Row> relationDs) {
        int indexDs = 1;
        while (BusCounter.countWithWheelchair(sparkSession, relationDs.limit(indexDs)) == 0) {
            ++indexDs;
        }
        return relationDs.limit(indexDs);

    }

    public void create(final SparkSession session, final Dataset<Row> relationDs) {
        ParquetWriter w = new ParquetWriter();
        w.write("relationDsTest.parquet", getMinRelationRowsNeeded(session, relationDs));
    }

}
