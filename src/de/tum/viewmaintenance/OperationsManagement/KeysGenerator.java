package de.tum.viewmaintenance.OperationsManagement;

import org.apache.cassandra.dht.LongToken;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.ByteBufferUtil;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigInteger;
import java.util.*;

/**
 * Created by shazra on 10/2/15.
 */
public class KeysGenerator {

    private static final org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(KeysGenerator.class);

    public static final String HASH_DATA_NODES = "data/ring_for_2nodesv2.txt";


    static int randInt(int min, int max) {
        Random rand = new Random();
        int randomNum = rand.nextInt((max - min) + 1) + min;
        return randomNum;
    }

    static LongToken generateRandomTokenMurmurPartition(int randKeyGenerated) {
        BigInteger bigInt = BigInteger.valueOf(randKeyGenerated);
        Murmur3Partitioner murmur3PartitionerObj = new Murmur3Partitioner();
        Token.TokenFactory tokenFac = murmur3PartitionerObj.getTokenFactory();
        LongToken generatedToken = murmur3PartitionerObj.getToken(ByteBufferUtil.bytes(randKeyGenerated));
        return generatedToken;
    }


    public static List<Integer> loadGenerationFromStaticKeyRangesFor2Nodes(int numTokensPerNode,
                                                                           String ip1, String ip2) {
        BufferedReader br = null;
        String sCurrentLine = null;
        List<Integer> bucketVM1 = new ArrayList<>();
        List<Integer> bucketVM2 = new ArrayList<>();
        List<Integer> finalBucket = new ArrayList<>();
        Map<String, List<Integer>> finalMap = new HashMap<>();
        try {
            br = new BufferedReader(
                    new FileReader(HASH_DATA_NODES));
            List<String> rangesFromFile = new ArrayList<>();


            List<LongToken> tokensVM1 = new ArrayList<>();
            List<LongToken> tokensVM2 = new ArrayList<>();

            while ( (sCurrentLine = br.readLine()) != null ) {
                rangesFromFile.add(sCurrentLine.trim());
            }
//            System.out.println(rangesFromFile);
            while ( bucketVM1.size() < numTokensPerNode || bucketVM2.size() < numTokensPerNode ) {
                int randKeyGenerated = randInt(0, 999);
                LongToken tokenGenerated = generateRandomTokenMurmurPartition(randKeyGenerated);
                String prevString = "";
                boolean prevCheck = false;
                for ( String tempStringFromFile : rangesFromFile ) {
                    String tempArr[] = tempStringFromFile.split(":");
                    String tempTokenFile = tempArr[1];
                    String currentIp = tempArr[0];
                    Murmur3Partitioner murmur3PartitionerObj = new Murmur3Partitioner();
                    Token.TokenFactory tokenFac = murmur3PartitionerObj.getTokenFactory();
                    Token limitToken = tokenFac.fromString(tempTokenFile);
//                    System.out.println("the limit token from the file is " + tempStringFromFile + " with index: " + rangesFromFile.indexOf(tempStringFromFile));

                    int comparisonValue = tokenGenerated.compareTo(limitToken);
                    // Comparing the generated token with that of the tokens in the file.
                    if ( comparisonValue >= 0 ) {
                        if ( comparisonValue == 0 ) {
                            if ( tempArr[0].equals(ip1) && bucketVM1.size() < numTokensPerNode ) {
//                                System.out.println("Equals case Satisfied here: " + tempStringFromFile);
                                if ( isUnique(randKeyGenerated, bucketVM1) ) {
                                    bucketVM1.add(randKeyGenerated);
                                    tokensVM1.add(tokenGenerated);
                                }
                                break;
                            }
                            if ( tempArr[0].equals(ip2) && bucketVM2.size() < numTokensPerNode ) {
//                                System.out.println("Equals case Satisfied here: " + tempStringFromFile);
                                if ( isUnique(randKeyGenerated, bucketVM2) ) {
                                    bucketVM2.add(randKeyGenerated);
                                    tokensVM2.add(tokenGenerated);
                                }
                                break;
                            }
                        } else if ( rangesFromFile.indexOf(tempStringFromFile) == rangesFromFile.size() - 1 ) {
                            System.out.println("Generating again for simplicity. Reached the last the element = " + tempStringFromFile);
                        } else {
                            prevCheck = true;
                            prevString = tempStringFromFile;
                        }
                    } else {
                        if ( prevCheck ) {
                            String prevStringArr[] = prevString.trim().split(":");
//                            if (prevStringArr[0].equals(ip1) && bucketVM1.size() < NUM_KEYS_GENERATED) {
                            if ( currentIp.equals(ip1) && bucketVM1.size() < numTokensPerNode ) {
//                                System.out.println("Satisfied here: " + prevString + " addding: " + randKeyGenerated);
//                                System.out.println("With generate token as " + tokenGenerated);
                                if ( isUnique(randKeyGenerated, bucketVM1) ) {
                                    bucketVM1.add(randKeyGenerated);
                                    tokensVM1.add(tokenGenerated);
                                }
                                break;
                            }
//                            if (prevStringArr[0].equals(ip2) && bucketVM2.size() < NUM_KEYS_GENERATED) {
                            if ( currentIp.equals(ip2) && bucketVM2.size() < numTokensPerNode ) {
//                                System.out.println("Satisfied here: " + prevString + " addding: " + randKeyGenerated);
//                                System.out.println("With generate token as " + tokenGenerated);
                                if ( isUnique(randKeyGenerated, bucketVM2) ) {
                                    bucketVM2.add(randKeyGenerated);
                                    tokensVM2.add(tokenGenerated);
                                }
                                break;
                            }
                        } else {
//                            System.out.println("I should never be here!!!");

                        }
                        prevCheck = false;

                    }

                }
            }


            finalMap.put(ip1, bucketVM1);
            finalMap.put(ip2, bucketVM2);
            finalBucket.addAll(bucketVM1);
            finalBucket.addAll(bucketVM2);
            logger.debug("### Generated keys :: " + finalMap);

        } catch ( Exception e ) {
            e.printStackTrace();
        } finally {
            try {
                if ( br != null )
                    br.close();
            } catch ( IOException ex ) {
                ex.printStackTrace();
            }
        }

        return finalBucket;

    }


    private static boolean isUnique(int elem, List<Integer> listKeys) {
        boolean isUnique = true;
        for ( int x : listKeys ) {
            if ( x == elem ) {
                isUnique = false;
                break;
            }
        }
        return isUnique;
    }

}
