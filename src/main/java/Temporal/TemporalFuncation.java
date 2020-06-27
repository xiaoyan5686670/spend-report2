package Temporal;


import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.functions.TemporalTableFunction;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.types.Row;

import java.lang.reflect.Type;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import static org.apache.flink.table.api.Expressions.$;

/*********************************
 @Author:xiaoyan.qin
 @Description:
 @Date:Created in 10:08 2020/6/26
 @Modified By:
 **********************************/

/**
 * 注意：在OLD Planner中，数据类型不支持BIGINT,想要使用BIGINT类型请使用，BLINK
 */
public class TemporalFuncation {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env,fsSettings);

        /**
        Provide a static data set of the rates history table.
         提供一个汇率历史数据表的静态数据
         */

        List<Tuple2<String,Long>> rateHistoryData = new ArrayList<>();
        rateHistoryData.add(Tuple2.of("US Dollar",102L));
        rateHistoryData.add(Tuple2.of("Euro",114L));
        rateHistoryData.add(Tuple2.of("Yen",1L));
        rateHistoryData.add(Tuple2.of("Euro",116L));
        rateHistoryData.add(Tuple2.of("Euro",119L));
        /**
         Provide a static data set of the rates order table.
         提供一个订单数据表
         */

        List<Tuple2<Integer,String>> OrdersData = new ArrayList<>();
        OrdersData.add(Tuple2.of(2,"Euro"));
        OrdersData.add(Tuple2.of(2,"US Dollar"));
        OrdersData.add(Tuple2.of(2,"Yen"));
        OrdersData.add(Tuple2.of(2,"Euro"));
        OrdersData.add(Tuple2.of(2,"US Dollar"));

        /**
         * Create and register an example table using above data set
         *  In the real setup,you should replace this with your own table.
         *  以上面两个数据集，创建和注册两个样列表
         *  在实际设置中，您应该用自己的表替换它。
         */
         /** 汇率历史表                            */

        DataStream<Tuple2<String,Long>> rateHistroyStream = env.fromCollection(rateHistoryData);
        Table rateHistory= tEnv.fromDataStream(rateHistroyStream,$("r_currency"),$("r_rate"),$("r_proctime").proctime());
        tEnv.createTemporaryView("RateHistory",rateHistory);
        TupleTypeInfo<Tuple3<String,Long,Date>> tupleType = new TupleTypeInfo<>(
                Types.STRING,Types.LONG, Types.SQL_TIMESTAMP
        );

        /**使用SQL查询数据*/
        Table revenue = tEnv.sqlQuery("SELECT * FROM RateHistory");
        tEnv.toRetractStream(revenue,tupleType).print(); //输出表数据

        /**使用TABLE API输出数据*/
        Table revenue2 = revenue.filter($("r_currency").isEqual("Euro")).groupBy($("r_currency")).select($("r_rate").sum());
        tEnv.toRetractStream(revenue2, Row.class).print();

        /**订单表                                  */
        DataStream<Tuple2<Integer,String>> OrdersStream = env.fromCollection(OrdersData);
        Table Orders = tEnv.fromDataStream(OrdersStream,$("amount"),$("currency"),$("r_proctime").proctime()); //使用TABLE API查询数据
        tEnv.toRetractStream(Orders,Row.class).print();   //输出表数据
        tEnv.createTemporaryView("Orders",Orders);   //注册一个视图

         /**
           create and register a temporal table function.
           Define "r_proctime" as the time attribute and "r_currency" as the primary key.
           创建并且注册一个临时表函数，定义"r_proctime" 做一个时间属性，并且"r_currency" 做为一个主键。
        **/

        TemporalTableFunction rates = rateHistory.createTemporalTableFunction("r_proctime","r_currency");

        //tEnv.registerFunction("Rates",rates); //registerFunction已经被抛弃
        tEnv.createTemporarySystemFunction("Rates",rates);


        TableResult qxy = tEnv.sqlQuery("SELECT" +
                "  o.amount * r.r_rate AS amount " +
                " FROM" +
                "  Orders AS o," +
                "  LATERAL TABLE (Rates(o.r_proctime)) AS r" +
                " WHERE r.r_currency = o.currency").execute();

        qxy.print();
        env.execute("qxy");


    }
}
