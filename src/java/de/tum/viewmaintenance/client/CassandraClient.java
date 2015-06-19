package de.tum.viewmaintenance.client;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;

/**
 * @ shazra on 5/28/15.
 */
public class CassandraClient {
    public static void main(String[] args) {
        {



            Cluster cluster;
            Session session;
            ResultSet results;
            Row rows;

            // Connect to the cluster and keyspace "demo"
            cluster = Cluster
                    .builder()
                    .addContactPoint("localhost")
                    .withRetryPolicy(DefaultRetryPolicy.INSTANCE)
                    .withLoadBalancingPolicy(
                            new TokenAwarePolicy(new DCAwareRoundRobinPolicy()))
                    .build();
            session = cluster.connect("schema1");

            // Create table
//        String query = "CREATE TABLE emp(emp_id int PRIMARY KEY, "
//                + "emp_name text, "
//                + "emp_city text, "
//                + "emp_sal varint, "
//                + "emp_phone varint );";
//        session.execute(query);

            // Delete table

            String query = "DROP TABLE emp";
            session.execute(query);


            // Insert one record into the users table
//        PreparedStatement statement = session.prepare(
//
//                "INSERT INTO users" + "(user_id, last, age,first)"
//                        + "VALUES (?, ?,?,?);");
//
//        BoundStatement boundStatement = new BoundStatement(statement);
//
//        session.execute(boundStatement.bind("userid" , "Jones", 35, "Bob"));

            // Use select to get the user we just entered
//        Statement select = QueryBuilder.select().all().from("users");
//        results = session.execute(select);
//        for (Row row : results) {
//            System.out.format("%s %d %s\n", row.getString("first"),
//                    row.getInt("age"), row.getString("user_id"));
//        }

//        // Update the same user with a new age
//        Statement update = QueryBuilder.update("demo", "users")
//                .with(QueryBuilder.set("age", 36))
//                .where((eq("lastname", "Jones")));
//        session.execute(update);
//
//        // Select and show the change
//        select = QueryBuilder.select().all().from("demo", "users")
//                .where(eq("lastname", "Jones"));
//        results = session.execute(select);
//        for (Row row : results) {
//            System.out.format("%s %d \n", row.getString("firstname"),
//                    row.getInt("age"));
//        }
//
//        // Delete the user from the users table
//        Statement delete = QueryBuilder.delete().from("users")
//                .where(eq("lastname", "Jones"));
//        results = session.execute(delete);
//
//
//        // Show that the user is gone
//        select = QueryBuilder.select().all().from("demo", "users");
//        results = session.execute(select);
//        for (Row row : results) {
//            System.out.format("%s %d %s %s %s\n", row.getString("lastname"),
//                    row.getInt("age"), row.getString("city"),
//                    row.getString("email"), row.getString("firstname"));
//        }

            // Clean up the connection by closing it
            cluster.close();
        }
    }
}
