import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.Cross;
import org.apache.spark.sql.functions;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;


public class AppStarter {

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

        nodeDs.show(2, false);
        relationDs.show(2, false);
        wayDs.show(2, false);

        System.out.println("All buses: " + BusCounter.countBuses(relationDs));
        System.out.println("Buses with wheelchair: " + BusCounter.countBusesWithWheelChair(sparkSession, relationDs));


        Dataset<Row> joinResult = DatasetCreator.createJoinedDs(nodeDs, wayDs);

        System.out.println("Crossing nodes: " + CrossingCounter.countAll(joinResult));
        System.out.println("Residential crossings: " + CrossingCounter.countCrossings(sparkSession, joinResult, "residential"));
        System.out.println("Crossing primary road: " + CrossingCounter.countCrossings(sparkSession, joinResult, "primary"));
        System.out.println("Crossing secondary road: " + CrossingCounter.countCrossings(sparkSession, joinResult, "secondary"));
}
}

