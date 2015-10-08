package de.tum.viewmaintenance.OperationsManagement;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.sun.org.apache.xpath.internal.operations.Bool;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by shazra on 10/2/15.
 */
public class OperationsGenerator {
    private static final org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(OperationsGenerator.class);

    private static final String CONFIG_FILE = "evaluationConfig.json";
    private int numOfOperations = 0;
    private int numOfKeys = 0;
    private boolean includeUpdates = false;
    private boolean includeDeletes = false;
    private int intervalOfFiringOperations = 0;
    private List<String> ipsInvolved = null;
    private List<Integer> insertKeysList = new ArrayList<>();
    private List<Integer> insertKeysSalList = new ArrayList<>();
    private String username = null;
    private String password = null;

    public List<String> getIpsInvolved() {
        return ipsInvolved;
    }

    public int getNumOfOperations() {
        return numOfOperations;
    }

    public int getNumOfKeys() {
        return numOfKeys;
    }

    public boolean isIncludeUpdates() {
        return includeUpdates;
    }

    public boolean isIncludeDeletes() {
        return includeDeletes;
    }

    public int getIntervalOfFiringOperations() {
        return intervalOfFiringOperations;
    }

    private OperationsGenerator() {
        try {
            readConfig();
        } catch ( IOException e ) {
            e.printStackTrace();
        }
    }

    private void readConfig() throws IOException {
        String stringList = new String(Files.readAllBytes(Paths.get(CONFIG_FILE)));
        Map<String, ?> configMap = new Gson().fromJson(stringList, new TypeToken<HashMap<String, Object>>() {
        }.getType());
        numOfOperations = ((Double) configMap.get("num_of_operations")).intValue();
        numOfKeys = ((Double) configMap.get("num_of_keys")).intValue();
        includeUpdates = (Boolean) configMap.get("include_updates");
        includeDeletes = (Boolean) configMap.get("include_deletes");
        intervalOfFiringOperations = ((Double) configMap.get("interval_of_firing_operations")).intValue();
        ipsInvolved = (List<String>) configMap.get("ips_involved");
        username = (String) configMap.get("username");
        password = (String) configMap.get("password");

    }

    public static OperationsGenerator getInstance() {
        return new OperationsGenerator();
    }

    public List<String> cqlGenerator(Boolean joinEnabled) {
        List<String> listOfOpertions = null;
        /**
         * Strategy 1: Randomize between insert(50%), update(25%) and delete(25%) if all are enabled.
         * Strategy 2: Randomize between insert(100%) if only insert is enabled.
         * Strategy 3: Randomize between insert(50%), update(50%) if delete is disabled.
         * Strategy 4: Randomize between insert(50%), delete(50%) if update is disabled.
         *
         * **/

        List<Integer> keysList = null;
        if ( ipsInvolved.size() == 2 ) {
            keysList = KeysGenerator.loadGenerationFromStaticKeyRangesFor2Nodes(numOfKeys, ipsInvolved.get(0),
                    ipsInvolved.get(1));
            if ( includeUpdates && includeDeletes ) {
                listOfOpertions = strategy1(keysList, joinEnabled);
            } else if ( includeUpdates ) {
                listOfOpertions = strategy3(keysList, joinEnabled);
            } else if ( includeDeletes ) {
                listOfOpertions = strategy4(keysList);
            } else if ( !includeUpdates && !includeDeletes ) {
                listOfOpertions = strategy2(keysList, joinEnabled);
            }
        }

//        logger.debug("#### List of Operations :: " + listOfOpertions);
        return listOfOpertions;
    }


    public List<String> strategy1(List<Integer> keysList, Boolean joinEnabled) {
        List<String> listOfOperations = new ArrayList<>();
        int operationsCount = 1;

        while ( operationsCount <= numOfOperations ) {

            int querySelector = OperationsUtils.getRandomInteger(1, 4);
            switch ( querySelector ) {
                case 1:
                case 4:
                    if ( joinEnabled ) {
                        int randomPicker = OperationsUtils.getRandomInteger(1, 2);
                        switch ( randomPicker ) {
                            case 1:
                                String tempInsertQuery = generateInsertQuery(keysList);
                                if ( tempInsertQuery != null ) {
                                    listOfOperations.add(tempInsertQuery);
                                    operationsCount++;
                                }
                                break;
                            case 2:
                                String tempInsertSalQuery = generateInsertSalTableQuery(keysList);
                                if ( tempInsertSalQuery != null ) {
                                    listOfOperations.add(tempInsertSalQuery);
                                    operationsCount++;
                                }
                                break;
                        }
                    } else {
                        String tempInsertQuery = generateInsertQuery(keysList);
                        if ( tempInsertQuery != null ) {
                            listOfOperations.add(tempInsertQuery);
                            operationsCount++;
                        }
                    }
                    break;
                case 2:
                    if ( joinEnabled ) {
                        int randomPicker = OperationsUtils.getRandomInteger(1, 2);
                        switch ( randomPicker ) {
                            case 1:
                                String tempUpdateQuery = generateUpdateQuery();
                                if ( tempUpdateQuery != null ) {
                                    listOfOperations.add(tempUpdateQuery);
                                    operationsCount++;
                                }
                                break;
                            case 2:
                                String tempUpdateSalQuery = generateUpdateQuerySalTable();
                                if ( tempUpdateSalQuery != null ) {
                                    listOfOperations.add(tempUpdateSalQuery);
                                    operationsCount++;
                                }
                                break;
                        }
                    } else {
                        String tempUpdateQuery = generateUpdateQuery();
                        if ( tempUpdateQuery != null ) {
                            listOfOperations.add(tempUpdateQuery);
                            operationsCount++;
                        }
                    }
                    break;
                case 3:
                    if ( joinEnabled ) {
                        int randomPicker = OperationsUtils.getRandomInteger(1, 2);
                        switch ( randomPicker ) {
                            case 1:
                                String tempDeleteQuery = generateDeleteQuery();
                                if ( tempDeleteQuery != null ) {
                                    listOfOperations.add(tempDeleteQuery);
                                    operationsCount++;
                                }
                                break;
                            case 2:
                                String tempDeleteSalQuery = generateDeleteSalQuery();
                                if ( tempDeleteSalQuery != null ) {
                                    listOfOperations.add(tempDeleteSalQuery);
                                    operationsCount++;
                                }
                                break;
                        }
                    } else {
                        String tempDeleteQuery = generateDeleteQuery();
                        if ( tempDeleteQuery != null ) {
                            listOfOperations.add(tempDeleteQuery);
                            operationsCount++;
                        }
                    }
                    break;

            }
        }
        return listOfOperations;
    }

    public List<String> strategy2(List<Integer> keysList, Boolean joinEnabled) {
        List<String> listOfOperations = new ArrayList<>();
        int operationsCount = 1;

        while ( operationsCount <= numOfOperations ) {
            if ( joinEnabled ) {
                int randomNum = OperationsUtils.getRandomInteger(1, 2);
                if ( randomNum == 1 ) {
                    // Prepare insert for emp
                    String tempInsertQuery = generateInsertQuery(keysList);
                    if ( tempInsertQuery != null ) {
                        listOfOperations.add(tempInsertQuery);
                        operationsCount++;
                    }
                } else {
                    String tempInsertQuery = generateInsertSalTableQuery(keysList);
                    if ( tempInsertQuery != null ) {
                        listOfOperations.add(tempInsertQuery);
                        operationsCount++;
                    }
                }
            } else {
                String tempInsertQuery = generateInsertQuery(keysList);
                if ( tempInsertQuery != null ) {
                    listOfOperations.add(tempInsertQuery);
                    operationsCount++;
                }
            }
        }
        return listOfOperations;
    }

    public List<String> strategy3(List<Integer> keysList, Boolean joinEnabled) {
        List<String> listOfOperations = new ArrayList<>();

        int operationsCount = 1;

        while ( operationsCount <= numOfOperations ) {

            int querySelector = OperationsUtils.getRandomInteger(1, 2);
            switch ( querySelector ) {
                case 1:
                    if ( joinEnabled ) {
                        int randomNum = OperationsUtils.getRandomInteger(1, 2);

                        switch ( randomNum ) {
                            case 1:
                                String tempInsertQuery = generateInsertQuery(keysList);
                                if ( tempInsertQuery != null ) {
                                    listOfOperations.add(tempInsertQuery);
                                    operationsCount++;
                                }
                                break;
                            case 2:
                                String tempSalInsertQuery = generateInsertSalTableQuery(keysList);
                                if ( tempSalInsertQuery != null ) {
                                    listOfOperations.add(tempSalInsertQuery);
                                    operationsCount++;
                                }
                                break;

                        }

                    } else {
                        String tempInsertQuery = generateInsertQuery(keysList);
                        if ( tempInsertQuery != null ) {
                            listOfOperations.add(tempInsertQuery);
                            operationsCount++;
                        }
                    }
                    break;
                case 2:
                    if ( joinEnabled ) {
                        int randomNum = OperationsUtils.getRandomInteger(1, 2);

                        switch ( randomNum ) {
                            case 1:
                                String tempUpdateQuery = generateUpdateQuery();
                                if ( tempUpdateQuery != null ) {
                                    listOfOperations.add(tempUpdateQuery);
                                    operationsCount++;
                                }
                                break;
                            case 2:
                                String tempSalUpdateQuery = generateUpdateQuerySalTable();
                                if ( tempSalUpdateQuery != null ) {
                                    listOfOperations.add(tempSalUpdateQuery);
                                    operationsCount++;
                                }
                                break;

                        }

                    } else {
                        String tempUpdateQuery = generateUpdateQuery();
                        if ( tempUpdateQuery != null ) {
                            listOfOperations.add(tempUpdateQuery);
                            operationsCount++;
                        }
                    }
                    break;
            }
        }
        return listOfOperations;
    }

    public List<String> strategy4(List<Integer> keysList) {
        List<String> listOfOperations = null;

        return listOfOperations;
    }

    private String generateInsertQuery(List<Integer> keysList) {
        String insertQuery = "insert into schematest.emp ( user_id, age, colaggkey_x, joinkey ) values " +
                "( $$pkey$$, $$age$$ , '$$colAggKey$$' , $$joinKey$$ )";

        boolean isFound = true;
        int insertLimit = 0;
        while ( isFound ) {
            int keyIndicator = OperationsUtils.getRandomInteger(0, keysList.size() - 1);

            int actualKey = keysList.get(keyIndicator);
            if ( !hasInsertedBefore(actualKey) ) {
                insertKeysList.add(actualKey);
                insertQuery = StringUtils.replace(insertQuery, "$$pkey$$", actualKey + "");
                insertQuery = StringUtils.replace(insertQuery, "$$age$$",
                        OperationsUtils.getRandomInteger(20, 40) + "");
                insertQuery = StringUtils.replace(insertQuery, "$$colAggKey$$",
                        "x" + OperationsUtils.getRandomInteger(1, 5));

                insertQuery = StringUtils.replace(insertQuery, "$$joinKey$$",
                        OperationsUtils.getRandomInteger(9970, 9999) + "");

                isFound = false;
            }

            insertLimit++;
            if ( insertLimit == keysList.size() * 2 ) {
                return null;
            }
        }

//        logger.debug("#### insert Query :: " + insertQuery);

        return insertQuery;
    }

    private String generateInsertSalTableQuery(List<Integer> keysList) {
        String insertQuery = "insert into schematest.salary ( user_id, salaryval, colaggkey_x, joinkey ) values " +
                "( $$pkey$$, $$sal$$ , '$$colAggKey$$' , $$joinKey$$ )";

        boolean isFound = true;
        int insertLimit = 0;
        while ( isFound ) {
            int keyIndicator = OperationsUtils.getRandomInteger(0, keysList.size() - 1);

            int actualKey = keysList.get(keyIndicator);
            if ( !hasInsertedBeforeSalTable(actualKey) ) {
                insertKeysSalList.add(actualKey);
                insertQuery = StringUtils.replace(insertQuery, "$$pkey$$", actualKey + "");
                insertQuery = StringUtils.replace(insertQuery, "$$sal$$",
                        OperationsUtils.getRandomInteger(2000, 4000) + "");
                insertQuery = StringUtils.replace(insertQuery, "$$colAggKey$$",
                        "x" + OperationsUtils.getRandomInteger(1, 5));

                insertQuery = StringUtils.replace(insertQuery, "$$joinKey$$",
                        OperationsUtils.getRandomInteger(9970, 9999) + "");

                isFound = false;
            }

            insertLimit++;
            if ( insertLimit == keysList.size() * 2 ) {
                return null;
            }
        }

//        logger.debug("#### insert Query :: " + insertQuery);

        return insertQuery;

    }

    private boolean hasInsertedBefore(int key) {
        for ( int keyEntry : insertKeysList ) {
            if ( keyEntry == key ) {
                return true;
            }
        }

        return false;
    }

    private boolean hasInsertedBeforeSalTable(int key) {
        for ( int keyEntry : insertKeysSalList ) {
            if ( keyEntry == key ) {
                return true;
            }
        }

        return false;
    }

    private String generateUpdateQuery() {
        String updateQuery = "insert into schematest.emp ( user_id, age, colaggkey_x, joinkey ) values " +
                "( $$pkey$$, $$age$$ , '$$colAggKey$$' , $$joinKey$$ )";

        if ( insertKeysList == null || insertKeysList.size() <= 0 ) {
            return null;
        }

        int keyIndicator = OperationsUtils.getRandomInteger(0, insertKeysList.size() - 1);

        int actualKey = insertKeysList.get(keyIndicator);

        updateQuery = StringUtils.replace(updateQuery, "$$pkey$$", actualKey + "");
        updateQuery = StringUtils.replace(updateQuery, "$$age$$",
                OperationsUtils.getRandomInteger(20, 40) + "");
        updateQuery = StringUtils.replace(updateQuery, "$$colAggKey$$",
                "x" + OperationsUtils.getRandomInteger(1, 5));

        updateQuery = StringUtils.replace(updateQuery, "$$joinKey$$",
                OperationsUtils.getRandomInteger(9970, 9999) + "");

        return updateQuery;
    }

    private String generateUpdateQuerySalTable() {
        String updateQuery = "insert into schematest.salary ( user_id, salaryval, colaggkey_x, joinkey ) values " +
                "( $$pkey$$, $$sal$$ , '$$colAggKey$$' , $$joinKey$$ )";

        if ( insertKeysSalList == null || insertKeysSalList.size() <= 0 ) {
            return null;
        }

        int keyIndicator = OperationsUtils.getRandomInteger(0, insertKeysSalList.size() - 1);

        int actualKey = insertKeysSalList.get(keyIndicator);

        updateQuery = StringUtils.replace(updateQuery, "$$pkey$$", actualKey + "");
        updateQuery = StringUtils.replace(updateQuery, "$$sal$$",
                OperationsUtils.getRandomInteger(2000, 4000) + "");
        updateQuery = StringUtils.replace(updateQuery, "$$colAggKey$$",
                "x" + OperationsUtils.getRandomInteger(1, 5));

        updateQuery = StringUtils.replace(updateQuery, "$$joinKey$$",
                OperationsUtils.getRandomInteger(9970, 9999) + "");

        return updateQuery;
    }

    private String generateDeleteQuery() {
        String deleteQuery = "delete from schematest.emp where user_id = $$pkey$$";

        if ( insertKeysList == null || insertKeysList.size() < 1 ) {
            return null;
        }

        int pkkeyPicker = OperationsUtils.getRandomInteger(0, insertKeysList.size() - 1);

        deleteQuery = deleteQuery.replace("$$pkey$$", insertKeysList.get(pkkeyPicker) + "");

        insertKeysList.remove(pkkeyPicker);

        return deleteQuery;
    }

    private String generateDeleteSalQuery() {
        String deleteQuery = "delete from schematest.salary where user_id = $$pkey$$";

        if ( insertKeysSalList == null || insertKeysSalList.size() < 1 ) {
            return null;
        }
        int pkkeyPicker = OperationsUtils.getRandomInteger(0, insertKeysSalList.size() - 1);

        deleteQuery = deleteQuery.replace("$$pkey$$", insertKeysSalList.get(pkkeyPicker) + "");

        insertKeysSalList.remove(pkkeyPicker);

        return deleteQuery;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }
}
