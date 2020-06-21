package sql;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.*;

import java.util.Arrays;

public class sqltest2 {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

        // register Orders table in table environment
// ...
        DataSource<Order> Orders = env.fromCollection(Arrays.asList(
                new Order("qxy", "beer", 3,1592726256000L),
                new Order("李志刚", "diaper", 4,1592726257000L),
                new Order("常保平", "rubber", 2,1592726258000L)));
// specify table program
        tEnv.createTemporaryView("Orders", Orders);
        Table orders = tEnv.from("Orders"); // schema (a, b, c, rowtime)
        Table result  = orders.filter(and ($("user").isNotNull(),
                                           $("product").isNotNull(),
                                           $("amount").isNotNull()
                )).select($("user").lowerCase().as("a"),$("amount"),$("rowtime"))
                .window(Tumble.over(lit(1).hours()).on($("rowtime")).as("hourlyWindow"))
                .groupBy($("hourlyWindow"),$("a"))
                .select($("a"),$("hourlyWindow" ).end().as("hour"),$("amount").avg().as("avgBillingAmount"));
        DataSet<Row> result2 = tEnv.toDataSet(result, Row.class);
        result2.print();
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