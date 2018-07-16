package UDF;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.api.java.UDF1;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.util.List;


public class IsCrossingSecondary implements UDF1<Seq<Row>, Boolean> {

    @Override public Boolean call(Seq<Row> tags) {
        final List<Row> rows = JavaConversions.seqAsJavaList(tags);
        return rows.stream().filter(row -> row.getAs("key").equals("highway"))
                .anyMatch(row -> row.getAs("value").equals("secondary"));

    }

}
