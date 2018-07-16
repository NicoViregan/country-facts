import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class ParquetReader {
    public static Dataset<Row> read(final String filePath, final SparkSession sparkSession) {
        return sparkSession.read().parquet(filePath);
    }
}
