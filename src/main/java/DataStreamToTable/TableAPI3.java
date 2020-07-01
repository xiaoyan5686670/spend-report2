package DataStreamToTable;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import java.time.Duration;

import static org.apache.flink.configuration.CheckpointingOptions.CHECKPOINTS_DIRECTORY;
import static org.apache.flink.table.api.Expressions.*;
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

        table2.select($("id"), $("name").as("d")).execute().print();
        TableResult qxy = table2.select($("*")).execute();
        try (CloseableIterator<Row> it = qxy.collect()) {
            while (it.hasNext()) {
               System.out.println(it.next()); // collect same data
            }
        }
    }
}
