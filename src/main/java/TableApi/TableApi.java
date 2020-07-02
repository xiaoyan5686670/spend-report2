package TableApi;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;

import static org.apache.flink.table.api.DataTypes.*;


/*********************************
 @Author:xiaoyan.qin
 @Description:
 @Date:Created in 13:55 2020/6/28
 @Modified By:
 **********************************/
public class TableApi {
    public static void main(String[] args) throws Exception {
        //enviornment configuration
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env,bsSettings);

        DataStream<Order> Orders = env.fromElements(new Order(1, 1, 1, 1593325245000L), new Order(2, 2, 2, 1593325246000L));

        List<Order> Orders2 = new ArrayList<>();
        Orders2.add(new Order(1, 1, 1, 1593325245000L));
        Orders2.add(new Order(2, 2, 2, 1593325246000L));
     //   tEnv.createTemporaryView("Orders",Orders,"a,b,c");
        DataStream<Order> Orders3 = env.fromCollection(Orders2);

        Table orders = tEnv.fromDataStream(Orders3);

        Table counts = orders.groupBy($("f0")).select( $("f0").count().as("cnt"));
        //conversion to DataSet
        DataStream<Tuple2<Boolean, Row>> result = tEnv.toRetractStream(counts, Row.class);
        result.print();
        env.execute("qxy");

    }

    public static class Order {
        public Integer a;
        public Integer b;
        public Integer c;
        public Long rowtime;

        public Order(Integer a, Integer b, Integer c, Long rowtime) {
            this.a = a;
            this.b = b;
            this.c = c;
            this.rowtime = rowtime;
        }

        public Integer getA() {
            return a;
        }

        public void setA(Integer a) {
            this.a = a;
        }

        public Integer getB() {
            return b;
        }

        public void setB(Integer b) {
            this.b = b;
        }

        public Integer getC() {
            return c;
        }

        public void setC(Integer c) {
            this.c = c;
        }

        public Long getRowtime() {
            return rowtime;
        }

        public void setRowtime(Long rowtime) {
            this.rowtime = rowtime;
        }
    }
}