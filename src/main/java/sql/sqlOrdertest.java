package sql;


import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import static org.apache.flink.table.api.Expressions.*;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import java.util.Arrays;

public class sqlOrdertest {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

// register Orders table in table environment
// ...
        DataSource<Order> Orders = env.fromCollection(Arrays.asList(
                new Order(1L, "beer", 3),
                new Order(1L, "diaper", 4),
                new Order(3L, "rubber", 2)));
// specify table program
        tEnv.createTemporaryView("Orders",Orders);
        Table orders = tEnv.from("Orders"); // schema (a, b, c, rowtime)

        Table counts = orders
                .groupBy($("user"))
                .select($("user"), $("product").count().as("cnt"));

// conversion to DataSet
        DataSet<Row> result = tEnv.toDataSet(counts, Row.class);
         result.print();
    }
    public static class Order {
        public Long user;
        public String product;
        public int amount;

        public Order() {
        }

        public Order(Long user, String product, int amount) {
            this.user = user;
            this.product = product;
            this.amount = amount;
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
