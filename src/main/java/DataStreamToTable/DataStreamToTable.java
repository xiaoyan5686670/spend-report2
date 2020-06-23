package DataStreamToTable;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.*;

/*********************************
 @Author:xiaoyan.qin
 @Description:
 @Date:Created in 21:41 2020/6/23
 @Modified By:
 **********************************/
public class DataStreamToTable {
    public static void main(String[] args) throws Exception {
        /**
         * 构建Stream Table初始化环境
         */
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment fsTableEnv = StreamTableEnvironment.create(env,fsSettings);

        /**
         * 生成数据流元素
         */
        DataStream<Tuple2<Long,String>> stream =env.fromElements(new Tuple2<>(1L,"hello"),new Tuple2<>(2L,"qxy"));

        /**
         * 将给定的的流转换为表
         */
        Table table1 = fsTableEnv.fromDataStream(stream);
        /**
         * 对表进行TABLE API操作
         */
        Table result  =  table1.filter($("f0").isNotEqual(3L)).select($("f1"),$("f0"));
        /**
         * 转换表到类型为Tuple2<String, LONG> Retract流,通过TypeInformation
         */
        TupleTypeInfo<Tuple2<String, Long>> tupleType = new TupleTypeInfo<>(
                Types.STRING,
                Types.LONG);

        DataStream<Tuple2<Boolean, Tuple2<String, Long>>> result2 = fsTableEnv.toRetractStream(result,tupleType);
        result2.print();
        /**
         *  fsTableEnv.execute("qxy");没有相关的table操作，因此不能用TABLE的excecute
         */

        env.execute("qxy");

    }
}
