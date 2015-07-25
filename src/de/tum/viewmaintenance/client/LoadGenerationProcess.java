package de.tum.viewmaintenance.client;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import de.tum.viewmaintenance.viewsTableStructure.Column;
import de.tum.viewmaintenance.viewsTableStructure.Table;
import org.apache.cassandra.dht.LongToken;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.XMLConfiguration;

import java.io.*;
import java.math.BigInteger;
import java.util.*;

/**
 * Created by shazra on 6/26/15.
 */
public class LoadGenerationProcess {
    private final static String BASETABLE_CONFIG = "baseTableConfig.xml";
    private static final String CONFIG_FILE = "/home/anarchy/work/sources/cassandra/viewConfig.xml";
    public static final String HASH_DATA_NODES = "data/ring_for_2nodes.txt";

    public static void main(String[] args) {
        LoadGenerationProcess loadGenerationProcess = new LoadGenerationProcess();
        Load load = loadGenerationProcess.configFileReader();
        System.out.println("Length of the list of tables=" + load.getTables().size());
        for (Table table: load.getTables()) {
            System.out.println("Table Name = " + table.getName());
            System.out.println("schema name = " + table.getKeySpace());
            loadGenerationProcess.resetTestInfrastructure(table, "192.168.56.20");
        }

        loadGenerationProcess.resetViews("192.168.56.20");
        loadGenerationProcess.loadGenerationFromStaticKeyRangesFor2Nodes(50, "192.168.56.20", "192.168.56.21");
    }

    private void resetTestInfrastructure(Table table, String ip) {
        deleteInfrastructure(table, ip);
        createInfrastructure(table, ip);
    }

    private Views readViewConfig(){
        System.out.println("************************ Reading View config files ******************");
        XMLConfiguration config = new XMLConfiguration();
        config.setDelimiterParsingDisabled(true);
        Views viewsObj = Views.getInstance();
        try {
            config.load(CONFIG_FILE);

            System.out.println("testing=" + config.getList("tableDefinition.name"));


            List<String> views = config.getList("tableDefinition.name");
            String keyspaceName = config.getString("keyspace");
            viewsObj.setKeyspace(keyspaceName);

            System.out.println("views = " + views);
            List<Table> tempTableList = new ArrayList<>();
            for (int i = 0; i < views.size(); i++) {
                Table table = new Table();
                List<Column> columns = new ArrayList<>();
                String viewTableName = config.getString("tableDefinition(" + i + ").name");
                String tableActionType = config.getString("tableDefinition(" + i + ").actionType").trim();
                String tableBasedOn = config.getString("tableDefinition(" + i + ").basedOn").trim();
                String primaryKeyName = config.getString("tableDefinition(" + i + ").primaryKey.name");
                String primaryKeyDataType = config.getString("tableDefinition(" + i + ").primaryKey.dataType");
                table.setName(viewTableName);
                Column primaryKey = new Column();
                primaryKey.setName(primaryKeyName);
                primaryKey.setIsPrimaryKey(true);
                primaryKey.setDataType(primaryKeyDataType);
                columns.add(primaryKey);
                System.out.println("primary Key name: " + primaryKeyName + " and datatype: "+  primaryKeyDataType);
                List<String> coldefs = config.getList("tableDefinition(" + i + ").column.name");
                System.out.println("no. of columns present = " + coldefs.size());
                for (int x = 0; x < coldefs.size(); x++) {
                    Column col = new Column();
                    String colName = config.getString("tableDefinition(" + i + ").column(" + x + ").name");
                    String colDataType = config.getString("tableDefinition(" + i + ").column(" + x + ").dataType");

                    String colConstraint = config.getString("tableDefinition(" + i + ").column(" + x + ").constraint");
                    String correspondingColumn = config.getString("tableDefinition(" + i + ").column(" + x + ").correspondingColumn");

                    col.setName(colName);
                    col.setDataType(colDataType);
                    col.setConstraint(colConstraint);
                    col.setCorrespondingColumn(correspondingColumn);
                    columns.add(col);
                }

                table.setColumns(columns);
                table.setActionType(tableActionType);
                table.setBasedOn(tableBasedOn);
                table.setKeySpace(keyspaceName);
                System.out.println("Adding the table = " + table);
                tempTableList.add(table);
                viewsObj.setTables(tempTableList);
            }
        } catch (Exception cex) {
            cex.printStackTrace();
        }
        return viewsObj;
    }

    private void resetViews(String ip){
        Views views = readViewConfig();
        List<Table> tables = views.getTables();
        for (Table table: tables) {
            deleteTable(ip, table.getKeySpace(), table.getName());
        }

    }

    private void createInfrastructure(Table table, String ip) {
        createKeySpace(ip , table.getKeySpace());
        createTableInCassandra(ip, table);
        Table viewTable = CassandraClientUtilities.createDeltaViewTable(table);
        createTableInCassandra(ip, viewTable);
    }

    private void deleteInfrastructure(Table table, String ip) {
        deleteTable(ip, table.getKeySpace(), table.getName());
        deleteTable(ip, table.getKeySpace(), table.getName() + "_deltaview");
    }




    private  Load configFileReader() {
        Load load = new Load();
        XMLConfiguration config = new XMLConfiguration();
        config.setDelimiterParsingDisabled(true);
        List<Table> tableList = new ArrayList<>();
        config.setEncoding("UTF-8");
        List<String> ipList = new ArrayList<>();
        try {
            config.load("baseTableConfig.xml");
            System.out.println("nodeList = " + config.getString("name"));
            System.out.println("list nodes = " + config.getRoot().getChildrenCount());
            List<HierarchicalConfiguration.Node> rootChildren = config.getRoot().getChildren();
            Iterator iterator = rootChildren.iterator();
            String setUpName = "";
            int numOfKeysPerNode = 0;
            String keyStorageStrategy = "";
            while (iterator.hasNext()) {
                HierarchicalConfiguration.Node node = (HierarchicalConfiguration.Node) iterator.next();
//                System.out.println("testing = " + node.getName());
                if (node.getName().equals("name")) {
                    setUpName = (String) node.getValue();
                } else if (node.getName().equals("nodes")) {
                    List<HierarchicalConfiguration.Node> nodeList = (List<HierarchicalConfiguration.Node>) node.getChildren("node");
//                    System.out.println("nodes" + nodeList.size());
                    for (int i = 0; i < nodeList.size(); i++) {
                        ipList.add((String) nodeList.get(i).getChild(0).getValue());
                    }
                } else if (node.getName().equals("tables")) {
//                    System.out.println("tables = " + node.getChildren("table"));
//                    System.out.println("schema name = " + (String) ((HierarchicalConfiguration.Node)
//                            node.getChildren("schemaName").get(0)).getValue());
                    load.setSchemaName((String)((HierarchicalConfiguration.Node)
                            node.getChildren("schemaName").get(0)).getValue());
//                    System.out.println("table count = " + node.getChildrenCount("table"));
                    List<HierarchicalConfiguration.Node> nodeTableList = (List<HierarchicalConfiguration.Node>) node.getChildren("table");
                    for (int i = 0; i < nodeTableList.size(); i++) {
                        Table t = new Table();
                        List<Column> columnList = new ArrayList<>();
                        List<HierarchicalConfiguration.Node> tablePropertiesList = (List<HierarchicalConfiguration.Node>) nodeTableList.get(i).getChildren();
                        for (int j = 0; j < tablePropertiesList.size(); j++) {
//                            System.out.println("Testing  ...." + (String) tablePropertiesList.get(j).getName());
                            if (((String) tablePropertiesList.get(j).getName()).equalsIgnoreCase("name")) {
                                t.setName((String) tablePropertiesList.get(j).getValue());
//                                System.out.println("table name = " + (String) tablePropertiesList.get(j).getValue());
                            } else if (((String) tablePropertiesList.get(j).getName()).equalsIgnoreCase("column")) {
                                List<HierarchicalConfiguration.Node> nodeColumnList = (List<HierarchicalConfiguration.Node>) tablePropertiesList.get(j).getChildren();
                                Column c = new Column();
                                for (int k = 0; k < nodeColumnList.size(); k++) {

                                    String nodeName = (String) nodeColumnList.get(k).getName();
                                    if (nodeName.equals("primaryKey")) {
                                        if (((String) nodeColumnList.get(k).getValue()).equalsIgnoreCase("true")) {
                                            c.setIsPrimaryKey(true);
                                        }
                                    } else if (nodeName.equals("name")) {
                                        c.setName((String) nodeColumnList.get(k).getValue());
                                    } else if (nodeName.equals("dataType")) {
                                        c.setDataType((String) nodeColumnList.get(k).getValue());
                                    }
                                }
                                System.out.println("Print column" + c);
                                columnList.add(c);
                            }
                        }
                        t.setKeySpace(load.getSchemaName());
                        t.setColumns(columnList);
                        tableList.add(t);

                        System.out.println("col list size = " + columnList.size());
                    }
                } else if (node.getName().equals("numOfKeysPerNode")) {
                    numOfKeysPerNode = Integer.parseInt((String) node.getValue());
                } else if (node.getName().equals("keysDistributionPattern")) {
                    keyStorageStrategy = (String) node.getValue();
                }

            }
            System.out.println(" are we really putting the list of tables = " + tableList.size());
            load.setTables(tableList);
            load.setIps(ipList);
            load.setNumTokensPerNode(numOfKeysPerNode);
            load.setStrategy(keyStorageStrategy);
        } catch (ConfigurationException e) {
            e.printStackTrace();
        }
        return load;
    }

    private boolean createKeySpace(String ip, String keyspaceName) {
        boolean isSuccessful = false;
        Cluster cluster = CassandraClientUtilities.getConnection(ip);
        CassandraClientUtilities.createKeySpace(cluster, keyspaceName);
        CassandraClientUtilities.closeConnection(cluster);
        return isSuccessful;
    }

    static LongToken generateRandomTokenMurmurPartition(int randKeyGenerated) {
        BigInteger bigInt = BigInteger.valueOf(randKeyGenerated);
        Murmur3Partitioner murmur3PartitionerObj = new Murmur3Partitioner();
        Token.TokenFactory tokenFac = murmur3PartitionerObj.getTokenFactory();
        LongToken generatedToken = murmur3PartitionerObj.getToken(ByteBufferUtil.bytes(randKeyGenerated));
        return generatedToken;
    }

    static int randInt(int min, int max) {
        Random rand = new Random();
        int randomNum = rand.nextInt((max - min) + 1) + min;
        return randomNum;
    }

    private void createKeys(List<String> ips, int numTokensPerNode, String strategy) {
        HashMap<String, HashMap<Integer, LongToken>> keyMap = new HashMap<>();
        if (strategy.equalsIgnoreCase("uniform")) {

        }
    }

    private List<Integer> loadGenerationFromStaticKeyRangesFor1Node(int numTokensPerNode, String ip1) {
        List<Integer> bucketVM1 = new ArrayList<>();
//        LongToken tokenGenerated = generateRandomTokenMurmurPartition(randKeyGenerated);
        while (bucketVM1.size()< numTokensPerNode) {
            int randKeyGenerated = randInt(0, 999);
            bucketVM1.add(randKeyGenerated);
        }
        return bucketVM1;
    }

    private void loadGenerationFromStaticKeyRangesFor2Nodes(int numTokensPerNode, String ip1, String ip2){
        BufferedReader br = null;
        String sCurrentLine = null;
        List<Integer> bucketVM1 = new ArrayList<>();
        List<Integer> bucketVM2 = new ArrayList<>();
        try {
            br = new BufferedReader(
                    new FileReader(HASH_DATA_NODES));
            List<String> rangesFromFile = new ArrayList<>();


            List<LongToken> tokensVM1 = new ArrayList<>();
            List<LongToken> tokensVM2 = new ArrayList<>();

            while ((sCurrentLine = br.readLine()) != null) {
                rangesFromFile.add(sCurrentLine.trim());
            }
//            System.out.println(rangesFromFile);
            while (bucketVM1.size() < numTokensPerNode || bucketVM2.size() < numTokensPerNode) {
                int randKeyGenerated = randInt(0, 999);
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
                            if (tempArr[0].equals(ip1) && bucketVM1.size() < numTokensPerNode) {
                                System.out.println("Equals case Satisfied here: " + tempStringFromFile);
                                if (isUnique(randKeyGenerated, bucketVM1)) {
                                    bucketVM1.add(randKeyGenerated);
                                    tokensVM1.add(tokenGenerated);
                                }
                                break;
                            }
                            if (tempArr[0].equals(ip2) && bucketVM2.size() < numTokensPerNode) {
                                System.out.println("Equals case Satisfied here: " + tempStringFromFile);
                                if (isUnique(randKeyGenerated, bucketVM2)) {
                                    bucketVM2.add(randKeyGenerated);
                                    tokensVM2.add(tokenGenerated);
                                }
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
                            if (currentIp.equals(ip1) && bucketVM1.size() < numTokensPerNode) {
                                System.out.println("Satisfied here: " + prevString + " addding: " + randKeyGenerated);
                                System.out.println("With generate token as " + tokenGenerated);
                                if (isUnique(randKeyGenerated, bucketVM1)) {
                                    bucketVM1.add(randKeyGenerated);
                                    tokensVM1.add(tokenGenerated);
                                }
                                break;
                            }
//                            if (prevStringArr[0].equals(ip2) && bucketVM2.size() < NUM_KEYS_GENERATED) {
                            if (currentIp.equals(ip2) && bucketVM2.size() < numTokensPerNode) {
                                System.out.println("Satisfied here: " + prevString + " addding: " + randKeyGenerated);
                                System.out.println("With generate token as " + tokenGenerated);
                                if (isUnique(randKeyGenerated, bucketVM2)) {
                                    bucketVM2.add(randKeyGenerated);
                                    tokensVM2.add(tokenGenerated);
                                }
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
//            createSchemaInCassandra(ip1);

            System.out.println("Inserting data for... " + ip1);
            Thread.sleep(5000);
            insertIntoCassandra(bucketVM1, ip1);


            System.out.println("Inserting data for... " + ip2);
            Thread.sleep(5000);
            insertIntoCassandra(bucketVM2, ip1);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (br != null)
                    br.close();
            } catch (IOException ex) {
                System.out.println(ex);
            }
        }
    }

    private boolean isUnique(int elem, List<Integer> listKeys) {
        boolean isUnique = true;
        for (int x: listKeys) {
            if (x == elem) {
                isUnique = false;
                break;
            }
        }
        return isUnique;
    }

    private boolean createTableInCassandra(String ip, Table table) {
        boolean isResultSuccessful = false;
        Cluster cluster = CassandraClientUtilities.getConnection(ip);
        CassandraClientUtilities.createTable(cluster, table);
        CassandraClientUtilities.closeConnection(cluster);
        return isResultSuccessful;
    }

    static boolean insertIntoCassandra(List<Integer> listOfKeys, String ip) {
        Cluster cluster = CassandraClientUtilities.getConnection(ip);
        for (int tempKey : listOfKeys) {
            String colAggKey_x = "x" + randInt(1, 5);
            CassandraClientUtilities.commandExecution(cluster, "INSERT INTO schematest.emp ( user_id , age, colAggKey_x ) values ( " + tempKey +
                    " , " + tempKey + ", '" + colAggKey_x + "' )");
        }
        CassandraClientUtilities.closeConnection(cluster);
        return true;
    }

    private boolean deleteTable(String ip, String keySpaceName, String tableName) {
        boolean isResultSucc = false;
        Cluster cluster = CassandraClientUtilities.getConnection(ip);
        CassandraClientUtilities.deleteTable(cluster, keySpaceName, tableName);
        CassandraClientUtilities.closeConnection(cluster);
        return isResultSucc;
    }

//    static boolean createTableEmpInCassandra(String ip) {
//        Cluster cluster = null;
//        Session session = null;
//        ResultSet results;
//        Row rows;
//
//        try {
//            // Connect to the cluster and keyspace "demo"
//            cluster = Cluster
//                    .builder()
//                    .addContactPoint(ip)
//                    .withRetryPolicy(DefaultRetryPolicy.INSTANCE)
//                    .withLoadBalancingPolicy(
//                            new TokenAwarePolicy(new DCAwareRoundRobinPolicy()))
//                    .build();
//            session = cluster.connect("schematest");
//
//            // Create table
//            String query = "CREATE TABLE emp(user_id int PRIMARY KEY, "
//                    + "age int);";
//            session.execute(query);
//        } catch (Exception e) {
//            e.printStackTrace();
//            return false;
//        } finally {
//            session.close();
//            cluster.close();
//        }
//
//        return true;
//    }

    static boolean deleteTableInCassandra(String tableName, String ip1) {
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
            session = cluster.connect("schematest");

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
