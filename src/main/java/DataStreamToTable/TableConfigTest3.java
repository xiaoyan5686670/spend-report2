package DataStreamToTable;


import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import org.apache.flink.table.api.EnvironmentSettings;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;

import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;


import java.time.Duration;
import java.util.Arrays;

import static org.apache.flink.table.api.Expressions.*;
import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Tumble.over;

/*********************************
 @Author:xiaoyan.qin
 @Description:
 @Date:Created in 15:35 2020/7/1
 @Modified By:
 **********************************/
public class TableConfigTest3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env,bsSettings);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        // register Orders table in table environment
// ...
        DataStream<Order> Orders = env.fromCollection(Arrays.asList(
                new Order("qxy", "beer", 3,1592726256000L),
                new Order("李志刚", "diaper", 4,1592726257000L),
                new Order("常保平", "rubber", 2,1592726258000L),
                new Order("qxy", "rubber", 2,1592726299000L)

        ));

        /**
         *  分配DataStream的流的时间戳,使用抛弃的API
         */
        DataStream<Order> stream  =Orders.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Order>(Time.seconds(10)) {
            @Override
            public long extractTimestamp(Order order) {
                return order.rowtime;
            }
        });
        /**
         * 分配DataStream的流的时间戳,使用新的API
         */
        WatermarkStrategy  test = WatermarkStrategy
                .<Order>forBoundedOutOfOrderness(Duration.ofSeconds(20))
                .withTimestampAssigner((event, timestamp) -> event.rowtime);
        DataStream<Order> stream2 = Orders.assignTimestampsAndWatermarks(test);
        /**
         * 方法1： 使用createTemoraryView创建视图,输出使用数据流
         */
        System.out.println("-------------------1-----------------------");
        tEnv.createTemporaryView("Orders", stream, $("user"), $("product"),$("amount"), $("rowtime").rowtime());
        Table counts1= tEnv.from("Orders").groupBy($("user")).select($("user"), $("product").count().as("cnt"));
        DataStream<Tuple2<Boolean, Row>> result1 = tEnv.toRetractStream(counts1, Row.class);
        result1.print();

        /**
         * 方法2： 使用fromDataStream创建视图,输出使用数据流
         */
        System.out.println("-------------------2-----------------------");

        Table orders = tEnv.fromDataStream(stream2, $("user"), $("product"),$("amount"),$("rowtime").rowtime());
        Table counts2=     orders.groupBy($("user")).select($("user"), $("product").count().as("cnt"));
        DataStream<Tuple2<Boolean, Row>> result2 = tEnv.toRetractStream(counts2, Row.class);
        result2.print();
        /**
         * 直接使用execut(),输出数据表,可以直接输出 ，而不用等到env.execute("q");
         */
        System.out.println("-------------------3-----------------------");
        //滚动窗口1分钟为一个单位
        orders.window(over(lit(1).minutes()).on($("rowtime")).as("userActionWindow"))
              .groupBy($("user"),$("userActionWindow"))
                .select($("user"),$("amount").sum().as("avgBillingAmount")).execute().print();
        //滚动窗口3分钟为一个单位
        orders.window(Tumble.over(lit(3).minutes()).on($("rowtime")).as("userActionWindow")).groupBy($("userActionWindow"),$("user"))
                .select($("user"),$("userActionWindow").end().as("hour"),$("amount").sum().as("avgBillingAmount")).execute().print();


        env.execute("q");
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
