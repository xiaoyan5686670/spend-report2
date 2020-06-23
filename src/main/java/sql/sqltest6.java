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
    public static void main(String[] args){
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        StreamExecutionEnvironment fsEnv = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        StreamTableEnvironment fsTableEnv = StreamTableEnvironment.create(fsEnv, fsSettings);

        final Schema schema = new Schema().field("count", DataTypes.INT()).field("word",DataTypes.STRING());

        fsTableEnv.connect(new FileSystem())
                .withFormat(new OldCsv().deriveSchema())
                .withSchema(schema)
                .createTemporaryTable("MySouce1");

        fsTableEnv.connect(new FileSystem())
                .withFormat(new OldCsv().deriveSchema())
                .withSchema(schema)
                .createTemporaryTable("MySource2");

        fsTableEnv.connect(new FileSystem())
                .withFormat(new OldCsv().deriveSchema())
                .withSchema(schema)
                .createTemporaryTable("MySink1");

        fsTableEnv.connect(new FileSystem())
                .withFormat(new OldCsv().deriveSchema())
                .withSchema(schema)
                .createTemporaryTable("MySink2");

        Table table1= fsTableEnv.from("MySource1").where($("word").like("F%"));
        table1.intersect("MySink1");



    }
}
