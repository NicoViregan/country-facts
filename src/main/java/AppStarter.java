import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import utilities.RoadTypes;


public class AppStarter {

    public static void main(final String[] args) {
        SparkSession sparkSession = SparkSession.builder().master("local[*]").appName("CountryFacts")
                .config("spark.sql.parquet.binaryAsString", "true").getOrCreate();
        sparkSession.sparkContext().setLogLevel("ERROR");


        final String nodePath = args[0];
        final String relationPath = args[1];
        final String wayPath = args[2];

        final Dataset<Row> nodeDs = ParquetReader.read(nodePath, sparkSession);
        final Dataset<Row> relationDs = ParquetReader.read(relationPath, sparkSession);
        final Dataset<Row> wayDs = ParquetReader.read(wayPath, sparkSession);

        System.out.println("All buses: " + BusCounter.countAll(relationDs));
        System.out.println("Buses with wheelchair: " + BusCounter.countWithWheelchair(sparkSession, relationDs));
        System.out.println();


        Dataset<Row> joinResult = NodeWayMerger.createJoinedDs(nodeDs, wayDs);
        CrossingCounter counter = new CrossingCounter(sparkSession);
        System.out.println("Crossing nodes: " + counter.countAll(joinResult));
        System.out.println("Residential crossings: " + counter
                .countByHighwayType(joinResult, RoadTypes.RESIDENTIAL.toString().toLowerCase()));
        System.out.println("Crossing on primary road: " + counter
                .countByHighwayType(joinResult, RoadTypes.PRIMARY.toString().toLowerCase()));
        System.out.println("Crossing on secondary road: " + counter
                .countByHighwayType(joinResult, RoadTypes.SECONDARY.toString().toLowerCase()));
        System.out.println("Crossing on trunk road: " + counter.countByHighwayType(joinResult, RoadTypes.TRUNK.toString().toLowerCase()));
        System.out.println("Crossing on motorway road: " + counter.countByHighwayType(joinResult, RoadTypes.MOTORWAY.toString().toLowerCase()));
    }
}

