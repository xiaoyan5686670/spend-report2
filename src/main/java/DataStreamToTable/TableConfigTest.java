package DataStreamToTable;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
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
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env,fsSettings);

        // obtain query configuration from TableEnvironment
//       TableConfig tConfig = tEnv.getConfig();
//
//        tEnv.getConfig().addConfiguration(
//                new Configuration()
//                        .set(CoreOptions.DEFAULT_PARALLELISM, 10)
//                        .set(PipelineOptions.AUTO_WATERMARK_INTERVAL, Duration.ofMillis(800))
//                        .set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(30))
//                        .set(CHECKPOINTS_DIRECTORY,"file:///d://tmp//")
//        );
        tEnv.getConfig().getConfiguration().set(
                ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE);
        tEnv.getConfig().getConfiguration().set(
                ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(10));
       System.out.println( tEnv.getConfig().getConfiguration().toString());
//        final Schema schema = new Schema()
//                .field("a", DataTypes.STRING())
//                .field("b", DataTypes.INT())
//                .field("c", DataTypes.BIGINT())
//                .field("rowtime",DataTypes.BIGINT()); //此处类型不能为：TIMESTAMP
//       tEnv.connect(new FileSystem().path("c:\\tmp\\qxy4.csv"))//CSV中的时间戳1592726259000L不用加L,否则报originated by LongParser: NUMERIC_VALUE_ILLEGAL_CHARACTER.
//                .withFormat(new OldCsv().fieldDelimiter("|").deriveSchema())
//                .withSchema(schema)
//                .createTemporaryTable("Orders");



        tEnv.executeSql("CREATE TABLE Orders (a VARCHAR, b INT, c BIGINT,user_action_time  TIMESTAMP(3)," +
                "  WATERMARK FOR user_action_time AS user_action_time - INTERVAL '5' SECOND " +
                ") WITH ('connector' = 'filesystem', " +
                  "  'path' = 'c://tmp//qxy4.csv', " +
                " 'format' = 'csv', " +
                " 'csv.field-delimiter' = ',' , "+
                " 'csv.ignore-parse-errors' = 'true'," +
                 " 'format.derive-schema' = 'true', " +
                " 'csv.allow-comments' = 'true')");

        tEnv.executeSql("CREATE TABLE Orders2 (a VARCHAR, b BIGINT" +
                "   " +
                ") WITH ('connector' = 'filesystem', " +
                "  'path' = 'c://tmp//qxy5', " +
                " 'format' = 'csv', " +
                " 'csv.field-delimiter' = ',' , "+
                " 'csv.ignore-parse-errors' = 'true'," +
                " 'format.derive-schema' = 'true', " +
                " 'csv.allow-comments' = 'true')");


        tEnv.executeSql("CREATE TABLE Orders3 (a VARCHAR, b BIGINT,dt VARCHAR ) " +
                " PARTITIONED BY (dt )  " +
                " WITH ('connector' = 'filesystem', " +
                "  'path' = 'c://tmp//qxy6', " +
                " 'format' = 'csv', " +
                " 'csv.field-delimiter' = ',' , "+
                " 'csv.ignore-parse-errors' = 'true'," +
                " 'format.derive-schema' = 'true', " +
                " 'csv.allow-comments' = 'true')");
        tEnv.executeSql("CREATE TABLE Orders4 (a VARCHAR, b BIGINT ) " +
                "   " +
                " WITH ( " +
                 "     'connector.type' = 'jdbc'," +

                "  'connector.url' = 'jdbc:mysql://localhost:3306/test', " +
                " 'connector.table' = 'pvuv_sink2', " +
                " 'connector.username' = 'root', " +
                " 'connector.password' = 'xy5686670', " +
                " 'connector.write.flush.max-rows' = '1' " +
                ")"
               ).print();
        tEnv.executeSql("show tables").print();
      //  TableResult tableResult1 = tEnv.executeSql("insert into Orders4 SELECT a,count(a) FROM Orders group by a  ");
        TableResult tableResult2 = tEnv.executeSql("insert into Orders4 SELECT a,count(a) FROM Orders group by  TUMBLE(user_action_time,INTERVAL '5' minutes),a  ");
      //  TableResult tableResult1 = tEnv.executeSql("insert into Orders3 SELECT a,count(1),'20200704' FROM Orders group by a ");
       // TableResult tableResult2 = tEnv.executeSql("insert into Orders4 SELECT a,count(1),'20200704' FROM Orders group by a ");
//        try (CloseableIterator<Row> it = tableResult1.collect()) {
//            while(it.hasNext()) {
//                Row row = it.next();
//               System.out.println(row);
//            }
//        }






    tEnv.execute("");
//
 env.execute("q");
    }
}
