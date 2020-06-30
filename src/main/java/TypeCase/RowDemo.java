package TypeCase;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/*********************************
 @Author:xiaoyan.qin
 @Description:
 @Date:Created in 10:41 2020/6/24
 @Modified By:
 **********************************/
public class RowDemo {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings fsSettings =  EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env= StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env,fsSettings);
        final Schema schema = new Schema().field("NAME", DataTypes.STRING()).field("word",DataTypes.BOOLEAN()).field( "TEST",DataTypes.BIGINT());


        DataStream<Row> stream =  env.fromElements(Row.of("hello", true, 1L),Row.of("qxy",false,3L));
        Table table = tEnv.fromDataStream(stream);
        /**
         * distinct()会被解析成group by
         * Table qxy =table.distinct();
         */
        /**
         * 创建SINK表
         */
        tEnv.connect(new FileSystem().path("c:\\tmp\\sink5"))
                .withFormat(new OldCsv().deriveSchema())
                .withSchema(schema)
                .createTemporaryTable("MySink2");
        /**
         * 使用弃用的insertInto,sink表必须存在
         *
         */



        table.insertInto("MySink2");


        tEnv.execute("q");




    }
}
