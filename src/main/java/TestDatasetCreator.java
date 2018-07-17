import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class TestDatasetCreator {

    public Dataset<Row> getBusWithWheelchairRow(SparkSession sparkSession, Dataset<Row> relationDs) {
        int indexDs = 1;
        while (BusCounter.countBusesWithWheelChair(sparkSession, relationDs.limit(indexDs)) == 0) {
            ++indexDs;
        }
        return relationDs.limit(indexDs);

    }

    public void create(SparkSession session, Dataset<Row> relationDs) {
        ParquetWriter w = new ParquetWriter();
        w.write("relationDsTest.parquet", getBusWithWheelchairRow(session, relationDs));
    }

}
