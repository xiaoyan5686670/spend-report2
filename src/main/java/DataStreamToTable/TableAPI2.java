package DataStreamToTable;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.*;
import static org.apache.flink.table.api.Expressions.and;

/*********************************
 @Author:xiaoyan.qin
 @Description:
 @Date:Created in 21:13 2020/6/29
 @Modified By:
 **********************************/
public class TableAPI2 {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

        //register Orders table in table environment

        //specify table program
        final Schema schema = new Schema()
                .field("a", DataTypes.STRING())
                .field("b", DataTypes.INT())
                .field("c", DataTypes.BIGINT())
                .field("rowtime",DataTypes.BIGINT()); //此处类型不能为：TIMESTAMP
        tEnv.connect(new FileSystem().path("d:\\tmp\\qxy4.csv"))//CSV中的时间戳1592726259000L不用加L,否则报originated by LongParser: NUMERIC_VALUE_ILLEGAL_CHARACTER.
                .withFormat(new OldCsv().fieldDelimiter("|").deriveSchema())
                .withSchema(schema)
                .createTemporaryTable("Orders");

        Table orders = tEnv.from("Orders"); // schema (a, b, c, rowtime)

        Table result = orders.filter(and(
                $("a").isNotNull(),
                $("b").isNotNull(),
                $("c").isNotNull()
        )).select($("a").lowerCase().as("a"),$("b"),$("rowtime"))
                .window(Tumble.over(lit(1).hours()).on($("rowtime")).as("hourlyWindow"))
                .groupBy($("hourlyWindow"),$("a")) //此处的"hourlyWinow为window函数中的定义的别名，要保持一致。
                .select($("a"),$("hourlyWindow").end().as("hour"),
                        $("b").avg().as("avgBillingAmount"));
        //DataSet<Row> result2 = tEnv.toDataSet(result, Row.class);
        result.execute().print();
    }
}
/**输出结果为*************************
 * +-----+-----------------------+------------------+
 * |   a |                  hour | avgBillingAmount |
 * +-----+-----------------------+------------------+
 * | cbp | 2020-06-21 08:00:00.0 |                3 |
 * | lzg | 2020-06-21 08:00:00.0 |                2 |
 * | qxy | 2020-06-21 08:00:00.0 |                2 |
 * +-----+-----------------------+------------------+
 ***********************************/




















