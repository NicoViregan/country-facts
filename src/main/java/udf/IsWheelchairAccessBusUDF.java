package udf;

import org.apache.spark.sql.Row;

import org.apache.spark.sql.api.java.UDF1;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.util.List;


public class IsWheelchairAccessBusUDF implements UDF1<Seq<Row>, Boolean> {


    @Override public Boolean call(Seq<Row> tags) {
        final List<Row> rows = JavaConversions.seqAsJavaList(tags);

        boolean isBus = rows.stream().anyMatch(row -> {
            final String key = row.getAs("key");
            final String value = row.getAs("value");
            return key.equals("route") && value.equals("bus");
        });

        boolean isWheelchair = rows.stream().anyMatch(row -> {
            final String key = row.getAs("key");
            final String value = row.getAs("value");
            return  key.equals("wheelchair") && value.equals("yes");
        });

        return isWheelchair && isBus;
    }
}
