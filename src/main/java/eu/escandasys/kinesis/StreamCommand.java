package eu.escandasys.kinesis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command
public class StreamCommand implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(StreamCommand.class);

    @Option(names = {"-s", "--start"}, description = "Whence to start from streaming data from archive", defaultValue = "now")
    String startWhence;

    @Option(names = {"-d", "--duration"}, description = "Time to stream data into stdout", defaultValue = "0")
    String durationStr;

    @Override
    public void run() {
        log.error("Starting streaming data...");
        log.error("\tStart: {}\n\tDuration: {}\n", startWhence, durationStr);
    }
}
