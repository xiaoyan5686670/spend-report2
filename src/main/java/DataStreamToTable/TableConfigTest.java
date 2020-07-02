package DataStreamToTable;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import java.time.Duration;

import static org.apache.flink.configuration.CheckpointingOptions.CHECKPOINTS_DIRECTORY;
import static org.apache.flink.table.api.Expressions.*;
import static org.apache.flink.table.api.Expressions.lit;

/*********************************
 @Author:xiaoyan.qin
 @Description:
 @Date:Created in 10:47 2020/7/1
 @Modified By:
 **********************************/
public class TableConfigTest {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        TableEnvironment tEnv = TableEnvironment.create(fsSettings);

        // obtain query configuration from TableEnvironment
       TableConfig tConfig = tEnv.getConfig();

        tEnv.getConfig().addConfiguration(
                new Configuration()
                        .set(CoreOptions.DEFAULT_PARALLELISM, 10)
                        .set(PipelineOptions.AUTO_WATERMARK_INTERVAL, Duration.ofMillis(800))
                        .set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(30))
                        .set(CHECKPOINTS_DIRECTORY,"file:///d://tmp//")
        );
       System.out.println( tEnv.getConfig().getConfiguration().toString());
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


        orders.select($("a").lowerCase().as("a"),$("b"),$("rowtime"))
                .window(Tumble.over(lit(1).hours()).on($("rowtime")).as("hourlyWindow"))
                .groupBy($("hourlyWindow"),$("a")) //此处的"hourlyWinow为window函数中的定义的别名，要保持一致。
                .select($("a"),$("hourlyWindow").end().as("hour"),
                        $("b").avg().as("avgBillingAmount")).execute().print();
        //DataSet<Row> result2 = tEnv.toDataSet(result, Row.class);

        // register TableSink

     //   result.execute().print();
        tEnv.execute("");

    }
}
