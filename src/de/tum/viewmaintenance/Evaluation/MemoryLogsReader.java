package de.tum.viewmaintenance.Evaluation;

import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
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
            JSch jsch = new JSch();
            Session session = jsch.getSession(username, host, PORT);
            session.setConfig("StrictHostKeyChecking", "no");
            session.setPassword(password);
            session.connect();

            ChannelExec channelExec = (ChannelExec) session.openChannel("exec");

            InputStream in = channelExec.getInputStream();

            channelExec.setCommand("grep Analysis ~/cassandraviewmaintenance/logs/system.log.*");
//            channelExec.setCommand("pwd");
            channelExec.connect();

            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            String line;

            while ( (line = reader.readLine()) != null ) {
                result.add(line);
            }

            int exitStatus = channelExec.getExitStatus();
            channelExec.disconnect();
            session.disconnect();

            if ( exitStatus < 0 ) {
                // System.out.println("Done, but exit status not set!");
            } else if ( exitStatus > 0 ) {
                // System.out.println("Done, but with error!");
            } else {
                // System.out.println("Done!");
            }

        } catch ( Exception e ) {
            System.err.println("Error: " + e);
        }
        return result;
    }
}
