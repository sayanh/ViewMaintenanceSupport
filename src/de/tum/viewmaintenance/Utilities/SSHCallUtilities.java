package de.tum.viewmaintenance.Utilities;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * Created by anarchy on 6/27/15.
 */
public class SSHCallUtilities {
    private final static String CASSANDRA_HOME = "/home/anarchy/work/sources/cassandra";
    private static void sshCallCassandra(String ip) {
        // Assumed to have a passwordless communication
        // Making an ssh call to Cassandra and getting the nodetool info is not a good idea
        // The out of the text is ill formatted and needs special text processing to get the token out of it.
        // This is a less priority thing hence will be dealt later.

        Process p = null;
        if (ip.equalsIgnoreCase("localhost") || ip.equalsIgnoreCase("127.0.0.1")) {
            String scriptLocation = CASSANDRA_HOME + "/bin/nodetool" ;
            try {
                p = new ProcessBuilder("" + scriptLocation, "ring").start();
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            // For different ips

        }
        String error = getStringFromInputStream(p.getErrorStream());
        String output = getStringFromInputStream(p.getInputStream());
        System.out.println("Error = \n" + error);
        System.out.println("Output = \n" + output);
    }

    // convert InputStream to String
    private static String getStringFromInputStream(InputStream is) {

        BufferedReader br = null;
        StringBuilder sb = new StringBuilder();

        String line;
        try {

            br = new BufferedReader(new InputStreamReader(is));
            while ((line = br.readLine()) != null) {
                sb.append(line);
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        return sb.toString();

    }
}
