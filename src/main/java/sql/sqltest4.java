package sql;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.ExplainDetail.ESTIMATED_COST;
import static org.apache.flink.table.api.Expressions.*;

public class sqltest4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        DataStream<Tuple2<Integer,String>> stream1 = env.fromElements(new Tuple2<>(1,"hello"));
        DataStream<Tuple2<Integer,String>> stream2 = env.fromElements(new Tuple2<>(1,"hello"));
        /**
         * TABLE api DEMO && 老计划解释
         */
        Table table1 = tEnv.fromDataStream(stream1,$("count"),$("word"));
        Table table2 = tEnv.fromDataStream(stream2,$("count"),$("word"));
        Table table = table1.where($("word").like("F%")).unionAll(table2);


        String explantion_old = tEnv.explain(table);

        System.out.println(explantion_old);
        /**
         * SQL写法 &&　新计划解释
         */
        tEnv.createTemporaryView("table1",table1);
        tEnv.createTemporaryView("table2",table2);
        String explantion_new = tEnv.explainSql("select * from table1 union all select * from table2",ESTIMATED_COST);
        System.out.println(explantion_new);
       // env.execute("qxy"); 在计划时不需要加execut
    }
}
