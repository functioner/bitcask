package bitcask.app;

import bitcask.KVStoreRemote;
import bitcask.KVStoreStub;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.jvm.hotspot.code.Stub;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Random;

public class Main {
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    private static Options getOptions() {
        final Options options = new Options();
        final Option path = new Option("p", "path", true, "path to persistent data");
        options.addOption(path);
        final Option server = new Option("s", "server", false, "server mode");
        options.addOption(server);
        final Option client = new Option("c", "client", false, "client mode");
        options.addOption(client);
        return options;
    }

    private static org.apache.commons.cli.CommandLine parseCommandLine(final String[] args) {
        final Options options = getOptions();
        try {
            return new org.apache.commons.cli.DefaultParser().parse(options, args);
        } catch (org.apache.commons.cli.ParseException e) {
            new org.apache.commons.cli.HelpFormatter().printHelp("utility-name", options);
            throw new RuntimeException("fail to parse the arguments");
        }
    }

    public static final void main(final String[] args) throws Exception {
        final CommandLine cmd = parseCommandLine(args);
        if (cmd.hasOption("client")) {
            final Random random = new Random(System.currentTimeMillis());
            final KVStoreRemote client = KVStoreStub.getClient("kv");
            for (int i = 0; i < 100; i++) {
                final int key = random.nextInt(10);
                if (random.nextBoolean()) {
                    final String value = client.get(key);
                    if (value == null) LOG.info("get key = {}, result = null", key);
                    else LOG.info("get key = {}, result = \"{}\"", key, value);
                } else {
                    final String value = "" + random.nextInt(100);
                    client.put(key, value);
                    LOG.info("put key = {}, value = \"{}\"", key, value);
                }
            }
        } else if (cmd.hasOption("server")) {
            final Path path = new File(cmd.getOptionValue("path")).toPath();
            try (final KVStoreStub server = new KVStoreStub("kv", 1099, path)) {
                LOG.info("Successfully started the KV store server...");
                while (true) {
                    Thread.sleep(100_000);
                }
            }
        }
    }
}
