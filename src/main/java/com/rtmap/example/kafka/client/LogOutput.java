package com.rtmap.example.kafka.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LogOutput    日志输出到Flume
 *
 * @author Muarine<maoyun@rtmap.com>
 * @date 2016 16/6/8 17:51
 * @since 0.1
 */
public class LogOutput {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogOutput.class);

    public static void main(String[] args) {

        for (int i = 0; i < 10 ; i++) {
            String log = "KAFKA-DATA:{\n" +
                    "\t\"head\":{\n" +
                    "\t\t\"id\":1,\n" +
                    "\t\t\"topic\":\"RT-GIVE-PRIZE\",\n" +
                    "\t\t\"host\":\"10.10.10.39\",\n" +
                    "\t\t\"tm\":\"2016-06-16 10:00:00\",\n" +
                    "\t\t\"v\":\"1.1.0\"\n" +
                    "\t},\n" +
                    "\t\"message\":{\n" +
                    "\t}\n" +
                    "}";

            LOGGER.info(log);

        }





    }

}
