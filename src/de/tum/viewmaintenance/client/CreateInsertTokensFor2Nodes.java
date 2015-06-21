package de.tum.viewmaintenance.client;


import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import org.apache.cassandra.dht.LongToken;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.ByteBufferUtil;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by shazra on 6/6/15.
 */
public class CreateInsertTokensFor2Nodes {
    final static int NUM_KEYS_GENERATED = 4;
    final static String ip1 = "192.168.56.20";
    final static String ip2 = "192.168.56.21";

    public static void main(String[] args) {
        BufferedReader br = null;
        String sCurrentLine = null;
        try {
            br = new BufferedReader(
                    new FileReader("data/ring_for_2nodes.txt"));
            List<String> rangesFromFile = new ArrayList<>();
            List<Integer> bucketVM1 = new ArrayList<>();
            List<Integer> bucketVM2 = new ArrayList<>();

            List<LongToken> tokensVM1 = new ArrayList<>();
            List<LongToken> tokensVM2 = new ArrayList<>();

            while ((sCurrentLine = br.readLine()) != null) {
                rangesFromFile.add(sCurrentLine.trim());
            }
//            System.out.println(rangesFromFile);
            while (bucketVM1.size() < NUM_KEYS_GENERATED || bucketVM2.size() < NUM_KEYS_GENERATED) {
                int randKeyGenerated = randInt(0, 9999999);
                LongToken tokenGenerated = generateRandomTokenMurmurPartition(randKeyGenerated);
//                System.out.println("the rand key generated: " + randKeyGenerated);
//                System.out.println("the token generated from the generated key is " + tokenGenerated);
                String prevString = "";
                boolean prevCheck = false;
                for (String tempStringFromFile : rangesFromFile) {
                    String tempArr[] = tempStringFromFile.split(":");
                    String tempTokenFile = tempArr[1];
                    String currentIp = tempArr[0];
                    Murmur3Partitioner murmur3PartitionerObj = new Murmur3Partitioner();
                    Token.TokenFactory tokenFac = murmur3PartitionerObj.getTokenFactory();
                    Token limitToken = tokenFac.fromString(tempTokenFile);
                    System.out.println("the limit token from the file is " + tempStringFromFile + " with index: " + rangesFromFile.indexOf(tempStringFromFile));

                    int comparisonValue = tokenGenerated.compareTo(limitToken);
                    // Comparing the generated token with that of the tokens in the file.
                    if (comparisonValue >= 0) {
                        if (comparisonValue == 0) {
                            if (tempArr[0].equals(ip1) && bucketVM1.size() < NUM_KEYS_GENERATED) {
                                System.out.println("Equals case Satisfied here: " + tempStringFromFile);
                                bucketVM1.add(randKeyGenerated);
                                tokensVM1.add(tokenGenerated);
                                break;
                            }
                            if (tempArr[0].equals(ip2) && bucketVM2.size() < NUM_KEYS_GENERATED) {
                                System.out.println("Equals case Satisfied here: " + tempStringFromFile);
                                bucketVM2.add(randKeyGenerated);
                                tokensVM2.add(tokenGenerated);
                                break;
                            }
                        } else if (rangesFromFile.indexOf(tempStringFromFile) == rangesFromFile.size() - 1) {
                            System.out.println("Generating again for simplicity. Reached the last the element = " + tempStringFromFile);
                        } else {
                            prevCheck = true;
                            prevString = tempStringFromFile;
                        }
                    } else {
                        if (prevCheck) {
                            String prevStringArr[] = prevString.trim().split(":");
//                            if (prevStringArr[0].equals(ip1) && bucketVM1.size() < NUM_KEYS_GENERATED) {
                            if (currentIp.equals(ip1) && bucketVM1.size() < NUM_KEYS_GENERATED) {
                                System.out.println("Satisfied here: " + prevString + " addding: " + randKeyGenerated);
                                System.out.println("With generate token as " + tokenGenerated);
                                bucketVM1.add(randKeyGenerated);
                                tokensVM1.add(tokenGenerated);

                                break;
                            }
//                            if (prevStringArr[0].equals(ip2) && bucketVM2.size() < NUM_KEYS_GENERATED) {
                            if (currentIp.equals(ip2) && bucketVM2.size() < NUM_KEYS_GENERATED) {
                                System.out.println("Satisfied here: " + prevString + " addding: " + randKeyGenerated);
                                System.out.println("With generate token as " + tokenGenerated);
                                bucketVM2.add(randKeyGenerated);
                                tokensVM2.add(tokenGenerated);

                                break;
                            }
                        } else {
                            System.out.println("I should never be here!!!");

                        }
                        prevCheck = false;

                    }

                }
            }


            System.out.println("The primary keys are -----------");
            System.out.println("For ip : " + ip1);
            System.out.println(bucketVM1);
            System.out.println(tokensVM1);
            System.out.println("For ip : " + ip2);
            System.out.println(bucketVM2);
            System.out.println(tokensVM2);

            // Insert into cassandra
            System.out.println("Creating schema in Cassandra");
            createSchemaInCassandra();

            System.out.println("Inserting data for... " + ip1);
            Thread.sleep(5000);
            insertIntoCassandra(bucketVM1);


            System.out.println("Inserting data for... " + ip2);
            Thread.sleep(5000);
            insertIntoCassandra(bucketVM2);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (br != null)
                    br.close();
            } catch (IOException ex) {
                System.out.println(ex);
                //ex.printStackTrace();
            }
        }
    }

    static LongToken generateRandomTokenMurmurPartition(int randKeyGenerated) {
        BigInteger bigInt = BigInteger.valueOf(randKeyGenerated);
        Murmur3Partitioner murmur3PartitionerObj = new Murmur3Partitioner();
        Token.TokenFactory tokenFac = murmur3PartitionerObj.getTokenFactory();
        LongToken generatedToken = murmur3PartitionerObj.getToken(ByteBufferUtil.bytes(randKeyGenerated));
        return generatedToken;
    }


    static int randInt(int min, int max) {

        // NOTE: Usually this should be a field rather than a method
        // variable so that it is not re-seeded every call.
        Random rand = new Random();

        // nextInt is normally exclusive of the top value,
        // so add 1 to make it inclusive
        int randomNum = rand.nextInt((max - min) + 1) + min;

        return randomNum;
    }


    static boolean insertIntoCassandra(List<Integer> listOfKeys) {
        Cluster cluster = null;
        Session session = null;
        ResultSet results;
        Row rows;

        try {


            // Connect to the cluster and keyspace "demo"
            cluster = Cluster
                    .builder()
                    .addContactPoint(ip1)
                    .withRetryPolicy(DefaultRetryPolicy.INSTANCE)
                    .withLoadBalancingPolicy(
                            new TokenAwarePolicy(new DCAwareRoundRobinPolicy()))
                    .build();
            session = cluster.connect("schema1");
            for (int tempKey : listOfKeys) {
                // Insert one record into a table
                PreparedStatement statement = session.prepare("INSERT INTO emp" + "(emp_id, emp_name)"
                        + "VALUES (?, ?);");

                BoundStatement boundStatement = new BoundStatement(statement);

                session.execute(boundStatement.bind(tempKey, "Jones" + tempKey));
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        } finally {
            session.close();
            cluster.close();
        }

        return true;
    }

    static boolean createSchemaInCassandra() {
        Cluster cluster = null;
        Session session = null;
        ResultSet results;
        Row rows;

        try {
            // Connect to the cluster and keyspace "demo"
            cluster = Cluster
                    .builder()
                    .addContactPoint(ip1)
                    .withRetryPolicy(DefaultRetryPolicy.INSTANCE)
                    .withLoadBalancingPolicy(
                            new TokenAwarePolicy(new DCAwareRoundRobinPolicy()))
                    .build();
            session = cluster.connect("schema1");

            // Create table
            String query = "CREATE TABLE emp(emp_id int PRIMARY KEY, "
                    + "emp_name text);";
            session.execute(query);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        } finally {
            session.close();
            cluster.close();
        }

        return true;
    }

    static boolean deleteTableInCassandra(String tableName) {
        Cluster cluster = null;
        Session session = null;
        ResultSet results;
        Row rows;

        try {
            // Connect to the cluster and keyspace "demo"
            cluster = Cluster
                    .builder()
                    .addContactPoint(ip1)
                    .withRetryPolicy(DefaultRetryPolicy.INSTANCE)
                    .withLoadBalancingPolicy(
                            new TokenAwarePolicy(new DCAwareRoundRobinPolicy()))
                    .build();
            session = cluster.connect("schema1");

            // Create table
            String query = "DROP TABLE " + tableName;
            session.execute(query);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        } finally {
            session.close();
            cluster.close();
        }

        return true;
    }

}
