package UDF;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.api.java.UDF2;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.util.List;


public class IsCrossingUDF implements UDF2<Seq<Row>, String, Boolean> {

    @Override public Boolean call(Seq<Row> tags, String tagValue) {
        final List<Row> rows = JavaConversions.seqAsJavaList(tags);
        return rows.stream().filter(row -> row.getAs("key").equals("highway"))
                .anyMatch(row -> row.getAs("value").equals(tagValue));
        }
}
