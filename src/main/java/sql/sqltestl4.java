package sql;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Arrays;

import static org.apache.flink.table.api.Expressions.$;

public class sqltestl4 {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);
        DataSource<Order> ds1 = env.fromCollection(Arrays.asList(
                new Order(1L, "a", 3),
                new Order(1L, "b", 4),
                new Order(3L, "c", 2)));
        DataSource<Order2> ds2 = env.fromCollection(Arrays.asList(
                new Order2(1L, "d", 3),
                new Order2(1L, "e", 4),
                new Order2(3L, "f", 2)));
        Table left = tEnv.fromDataSet(ds1, "a, b, c");
        Table right = tEnv.fromDataSet(ds2, "d, e, f");
        Table result = left.join(right)
                .where($("a").isEqual($("d")))
                .select($("a"), $("b"), $("e"));

        DataSet<Row> result2 = tEnv.toDataSet(result, Row.class);
        result2.print();

    }
    public static class Order {
        public Long a;
        public String b;
        public int c;

        public Order() {
        }

        public Order(Long a, String b, int c) {
            this.a = a;
            this.b = b;
            this.c = c;
        }

        @Override
        public String toString() {
            return "Order{" +
                    "user=" + a +
                    ", product='" + b + '\'' +
                    ", amount=" + c +
                    '}';
        }
    }
    public static class Order2 {
        public Long d;
        public String e;
        public int f;

        public Order2() {
        }

        public Order2(Long d, String e, int f) {
            this.d = d;
            this.e = e;
            this.f = f;
        }

        @Override
        public String toString() {
            return "Order{" +
                    "user=" + d +
                    ", product='" + e + '\'' +
                    ", amount=" + f +
                    '}';
        }
    }
}
