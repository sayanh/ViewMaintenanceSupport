package de.tum.viewmaintenance.Evaluation;

import com.jcraft.jsch.Session;
import de.tum.viewmaintenance.client.SshUtilities;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by shazra on 10/6/15.
 */
public class MemoryLogsReader {
    public static final int PORT = 22;
    public static List<String> getMemoryLogs(String host, String username, String password) {
        List<String> result = new ArrayList<String>();
        try {
            Session session = SshUtilities.getSession(username, password, host, PORT);

            result = SshUtilities.runCommand(session, "grep Analysis ~/cassandraviewmaintenance/logs/system.log");

            SshUtilities.sessionDisconnect(session);

        } catch ( Exception e ) {
            System.err.println("Error: " + e);
        }
        return result;
    }
}
