package sql;

/*********************************
 @Author:xiaoyan.qin
 @Description:
 @Date:Created in 15:05 2020/6/23
 @Modified By:
 **********************************/
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.types.DataType;

import static org.apache.flink.table.api.Expressions.$;


public class sqltest6 {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        StreamExecutionEnvironment fsEnv = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        StreamTableEnvironment fsTableEnv = StreamTableEnvironment.create(fsEnv, fsSettings);

        final Schema schema = new Schema().field("count", DataTypes.INT()).field("word",DataTypes.STRING());

        fsTableEnv.connect(new FileSystem().path("c:\\tmp\\qxy1.csv"))
                .withFormat(new OldCsv().deriveSchema())
                .withSchema(schema)
                .createTemporaryTable("MySource1");

        fsTableEnv.connect(new FileSystem().path("c:\\tmp\\qxy2.csv"))
                .withFormat(new OldCsv().deriveSchema())
                .withSchema(schema)
                .createTemporaryTable("MySource2");

        fsTableEnv.connect(new FileSystem().path("c:\\tmp\\sink1"))
                .withFormat(new OldCsv().deriveSchema())
                .withSchema(schema)
                .createTemporaryTable("MySink1");

        fsTableEnv.connect(new FileSystem().path("c:\\tmp\\sink2"))
                .withFormat(new OldCsv().deriveSchema())
                .withSchema(schema)
                .createTemporaryTable("MySink2");

        Table table1= fsTableEnv.from("MySource1").where($("word").like("F%"));
        table1.insertInto("MySink1");

        Table table2 = table1.unionAll(fsTableEnv.from("MySource2"));
        table2.insertInto("MySink2");
        String explanation = fsTableEnv.explain(false);
        System.out.println(explanation);
        fsTableEnv.execute("qxy");



    }
}


























