import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class TestStarter {

    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession.builder().master("local[*]").appName("CountryFacts")
                .config("spark.sql.parquet.binaryAsString", "true").getOrCreate();
        sparkSession.sparkContext().setLogLevel("ERROR");

        final String nodePath = args[0];
        final String relationPath = args[1];
        final String wayPath = args[2];

        final Dataset<Row> nodeDs = ParquetReader.read(nodePath, sparkSession);
        final Dataset<Row> relationDs = ParquetReader.read(relationPath, sparkSession);
        final Dataset<Row> wayDs = ParquetReader.read(wayPath, sparkSession);

        TestDatasetCreator creator = new TestDatasetCreator();
        creator.create(sparkSession, relationDs);
    }
}
