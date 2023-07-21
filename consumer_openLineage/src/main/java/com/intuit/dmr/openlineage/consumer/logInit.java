package com.intuit.dmr.openlineage.consumer;
import java.io.FileInputStream;
import java.util.*;
import java.util.logging.*;


public class logInit{
    public static class MyFormatter extends SimpleFormatter {
        @Override
        public String format(LogRecord record) {
            return String.format("%1$tF %1$tT [%4$s] %5$s%6$s%n",
                    new Date(record.getMillis()),
                    record.getSourceClassName(),
                    record.getSourceMethodName(),
                    record.getLevel().getLocalizedName(),
                    record.getMessage(),
                    System.getProperty("line.separator"));
        }
    }
    private static final Logger logger=Logger.getLogger(logInit.class.getName());
    public static void initializeLogger(){
        try{
            LogManager.getLogManager().readConfiguration(new FileInputStream("./src/main/resources/logging.properties"));
            logger.info("Initializing logger..");
        }catch (java.io.IOException e){
//           System.out.println("Error reading the configuration file, Setting logger properties manually");

           logger.setLevel(Level.INFO);
           ConsoleHandler handler = new ConsoleHandler();
           handler.setLevel(Level.INFO);
//            SimpleFormatter formatter = new SimpleFormatter() {
//                @Override
//                public String format(LogRecord record) {
//                    return String.format("%1$tF %1$tT [%4$s] %5$s%6$s%n",
//                            new Date(record.getMillis()),
//                            record.getSourceClassName(),
//                            record.getSourceMethodName(),
//                            record.getLevel().getLocalizedName(),
//                            record.getMessage(),
//                            System.getProperty("line.separator"));
//                }
//            };
            handler.setFormatter(new MyFormatter()); // Set the formatter
            logger.addHandler(handler);
            logger.warning("Error reading the logger configuration file");
//            System.out.println("Logger created!");
        }
    }
}
