package UDF;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;

import org.apache.spark.sql.api.java.UDF1;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.util.List;


public class IsWheelchairAccessBusUDF implements UDF1<Seq<Row>, Boolean> {


    @Override public Boolean call(Seq<Row> tags) {
        final List<Row> rows = JavaConversions.seqAsJavaList(tags);

        boolean isBus = rows.stream()
                .filter(row -> row.getAs("key").equals("route"))
                .anyMatch(row -> row.getAs("value").equals("bus"));

        boolean isWheelchair = rows.stream()
                .filter(row -> row.getAs("key").equals("wheelchair"))
                .anyMatch(row -> row.getAs("value").equals("yes"));
        return isWheelchair && isBus;

    }
}
