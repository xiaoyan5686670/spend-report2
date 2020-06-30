package DataStreamToTable;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;

import static org.apache.flink.table.api.Expressions.row;

/*********************************
 @Author:xiaoyan.qin
 @Description:
 @Date:Created in 21:40 2020/6/30
 @Modified By:
 **********************************/
public class TableAPI3 {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

        Table table = tEnv.fromValues(
                row(1, "ABC"),
                row(2L, "ABCDE")
        );
        table.printSchema();
        table.execute().print();

        Table table2 = tEnv.fromValues(
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.DECIMAL(10, 2)),
                        DataTypes.FIELD("name", DataTypes.STRING())
                ),
                row(1, "ABC"),
                row(2L, "ABCDE")
        );
        table2.printSchema();
        table2.execute().print();
    }
}
