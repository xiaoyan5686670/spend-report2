package DataStreamToTable;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/*********************************
 @Author:xiaoyan.qin
 @Description:
 @Date:Created in 20:52 2020/6/28
 @Modified By:
 **********************************/
public class Test1 {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);
        /**
         https://ci.apache.org/projects/flink/flink-docs-master/dev/table/common.html
         使用java pojo不行，所以使用schme类。原文件链接见上
         */
        final Schema schema = new Schema()
                .field("a", DataTypes.INT())
                .field("b", DataTypes.STRING())
                .field("c", DataTypes.BIGINT());

        tEnv.connect(new FileSystem().path("c:\\tmp\\qxy3.csv"))
                .withFormat(new OldCsv().fieldDelimiter("|").deriveSchema())
                .withSchema(schema)
                .createTemporaryTable("Orders");

        Table orders = tEnv.from("Orders"); // schema (a, b, c, rowtime)

        Table counts = orders
                .groupBy($("a"))
                .select($("a"), $("b").count().as("cnt"));

// conversion to DataSet
        DataSet<Row> result = tEnv.toDataSet(counts, Row.class);
        result.print();

    }
}
