package DataStreamToTable;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.GroupWindowedTable;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import sql.sqltest2;

import java.util.Arrays;

import static org.apache.flink.table.api.Expressions.*;
import static org.apache.flink.table.api.Expressions.$;

/*********************************
 @Author:xiaoyan.qin
 @Description:
 @Date:Created in 15:35 2020/7/1
 @Modified By:
 **********************************/
public class TableConfigTest2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env,bsSettings);

        // register Orders table in table environment
// ...
        DataStreamSource<Order> Orders = env.fromCollection(Arrays.asList(
                new Order("qxy", "beer", 3,1592726256000L),
                new Order("李志刚", "diaper", 4,1592726257000L),
                new Order("常保平", "rubber", 2,1592726258000L)));
// specify table program
//        DataStream<Order> stream  =Orders.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Order>(Time.seconds(10)) {
//            @Override
//            public long extractTimestamp(Order order) {
//                return order.rowtime;
//            }
//        });
       tEnv.createTemporaryView("Orders", Orders, $("user"), $("product"),$("amount"), $("rowtime").rowtime());
       Table orders = tEnv.from("Orders").select($("user").lowerCase().as("a"),$("amount"),$("rowtime").rowtime());
       orders.window(Tumble.over(lit(1).minutes()).on($("rowtime")).as("userActionWindow")).groupBy($("userActionWindow"),$("user"))
                .select($("user"),$("userActionWindow").end().as("hour"),$("amount").avg().as("avgBillingAmount")).execute().print();


    }
    public static class Order {
        public String user;
        public String product;
        public int amount;
        public Long rowtime;

        public Order() {
        }

        public Order(String user, String product, int amount,Long rowtime) {
            this.user = user;
            this.product = product;
            this.amount = amount;
            this.rowtime=rowtime;
        }

        @Override
        public String toString() {
            return "Order{" +
                    "user=" + user +
                    ", product='" + product + '\'' +
                    ", amount=" + amount +
                    '}';
        }
    }
}