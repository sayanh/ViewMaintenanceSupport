package de.tum.viewmaintenance.client;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import java.util.List;

/**
 * Created by shazra on 6/29/15.
 */
public class Demo {
    public static void main(String[] args) {
//        ResultSet resultSet = CassandraClientUtilities.getResultSet("localhost", "select * from schema2.vt2 where k = 1");
//        System.out.println("Resultset = " + resultSet);
//        System.out.println("Count = " + resultSet.all().size());
//        System.out.println("One = " + resultSet.one());
//        System.out.println("Execution info = " + resultSet.getExecutionInfo().getQueryTrace());
        String key = "x1";
        int x = 1;
        List<Row> rows = getRows("schema2", "vt2", QueryBuilder.eq("count_view1_age", x));
        System.out.println("size = " + rows.size());
        System.out.println("values 1st col = " + rows);
//        System.out.println("values 2st col = " + rows.get(0).getInt(1));
//        System.out.println("values 3st col = " + rows.get(0).getInt(2));
//        System.out.println("values 4st col = " + rows.get(0).getInt(3));
    }

    public static List<Row> getRows(String keyspace, String table , Clause equal) {
        Cluster cluster = null;
        Session session = null;
        List<Row> result = null;
        Statement statement = null;
        try {
            cluster = CassandraClientUtilities.getConnection("localhost");
            session = cluster.connect();

            if (equal != null) {
                statement = QueryBuilder
                        .select()
                        .all()
                        .from(keyspace, table).
                                where(equal);
            } else {
                statement = QueryBuilder
                        .select()
                        .all()
                        .from(keyspace, table);
            }

            result = session
                    .execute(statement)
                    .all();
            session.close();
            cluster.close();


        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (session.isClosed()) {
                session.close();
            }

            if (cluster.isClosed()) {
                cluster.close();
            }
        }
        return  result;
    }
}
