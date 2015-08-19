package de.tum.viewmaintenance.trials;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 * Created by anarchy on 8/18/15.
 */
public class Business {
    public String name;
    public File file;

    //    @Argument(metaVar = "[target [target2 [target3] ...]]", usage = "targets")
    @Argument(metaVar = "target", usage = "targets")
    private List<String> targets = new ArrayList<String>();

    @Option(name="-load",usage="load a file")
    private String load;

    @Option(name="-r",usage="recursively run something")
    private boolean recursive;

    @Option(name="-o",usage="output to this file",metaVar="OUTPUT")
    private File out = new File(".");

    @Option(name="-str")        // no usage
    private String str = "(default value)";

    @Option(name="-hidden-str2",hidden=true,usage="hidden option")
    private String hiddenStr2 = "(default value)";


    public void setFile(File f) {
        if (f.exists()) file = f;
    }

    public void run() {
        System.out.println("Business-Logic");
        System.out.println("- name: " + name);
        System.out.println("- file: " + ((file != null)
                ? file.getAbsolutePath()
                : "null"));
        while (true){
            Scanner scanner = new Scanner(System.in);
            String command = scanner.nextLine();
            System.out.println("Your message: " + command);
            CmdLineParser parser = new CmdLineParser(this);
            String args[] = command.split(" ");
            try {
                parser.parseArgument(args);
            } catch (CmdLineException e) {
                e.printStackTrace();
            }

            if (!targets.isEmpty()) {
                System.out.println("targets are " + targets);
                targets.clear();
            }
            if (load !=null && !load.isEmpty()) {
                System.out.println("LOading shit!!! " + load);
            }
        }


    }


    public static void main(String[] args) throws IOException {
        System.out.println(System.getProperty("user.dir"));
        String argStr = "";
        args = argStr.split(" ");
        new Business().run();
    }
}
