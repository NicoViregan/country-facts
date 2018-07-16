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

        //        nodeDs.show(2, false);
        //        relationDs.show(2, false);
        //        wayDs.show(2, false);

        //        System.out.println("All buses: " + BusCounter.countBuses(relationDs));
        //        System.out.println("Buses with wheelchair: " + BusCounter.countBusesWithWheelChair(sparkSession,
        // relationDs));
        //
        Dataset<Row> explodedWay = DatasetCreator.explodeNodes(wayDs);
        Dataset<Row> explodedWayWithIdAndIndexColumns = DatasetCreator.addIndexAndIdColumns(explodedWay);
        Dataset<Row> renameNodeDs = RenameDatasets.renameNodeDs(nodeDs);
        Dataset<Row> joinResult = DatasetCreator.join(explodedWayWithIdAndIndexColumns, renameNodeDs);

        System.out.println("Crossing nodes: " + CrossingCounter.countAll(joinResult));
        System.out.println("Residential crossings: " + CrossingCounter.countResidential(sparkSession, joinResult));
        System.out.println("Crossing primary road: " + CrossingCounter.countPrimary(sparkSession, joinResult));
        System.out.println("Crossing secondary road: " + CrossingCounter.countSecondary(sparkSession, joinResult));
    }
}

