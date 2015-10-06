package de.tum.viewmaintenance.client;

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
public final class SshUtilities {

    public static Session getSession(String username, String password, String host, int PORT) {

        ChannelExec channelExec = null;
        Session session = null;
        try {
            JSch jsch = new JSch();
            session = jsch.getSession(username, host, PORT);
            session.setConfig("StrictHostKeyChecking", "no");
            session.setPassword(password);
            session.connect();

        } catch ( Exception e ) {
            e.printStackTrace();
        }

        return session;
    }


    public static void sessionDisconnect(Session session) {
        if ( session != null && session.isConnected() ) {
            session.disconnect();
        }
    }


    public static List<String> runCommand(Session session, String command) {
        List<String> result = null;
        ChannelExec channelExec = null;
        try {
            channelExec = (ChannelExec) session.openChannel("exec");
            InputStream in = channelExec.getInputStream();
            channelExec.setCommand(command);
            channelExec.connect();

            int exitStatus = channelExec.getExitStatus();


            if ( exitStatus < 0 ) {
                // System.out.println("Done, but exit status not set!");
            } else if ( exitStatus > 0 ) {
                // System.out.println("Done, but with error!");
            } else {
                // System.out.println("Done!");
            }

            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            String line;
            result = new ArrayList<>();
            while ( (line = reader.readLine()) != null ) {
                result.add(line);
            }


        } catch ( Exception e ) {
            e.printStackTrace();
        } finally {
            if ( channelExec != null && channelExec.isConnected() ) {
                channelExec.disconnect();
            }
        }
        return result;
    }
}
