package com.lpc.realtime.warehouse.a9_test;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @Title: com.lpc.realtime.warehouse.a9_test.Log4j2Demo
 * @Package: PACKAGE_NAME
 * @Description:
 * @Author: lpc
 * @Date: 2023/12/8 14:25
 * @Version:1.0
 */
public class Log4j2Demo {
    public static final Logger logger = LogManager.getLogger(Log4j2Demo.class);
    public static void main(String[] args) {
        logger.trace("Trace level message");
        logger.debug("Debug level message");
        logger.info("Info level message");
        logger.warn("Warn level message");
        logger.error("Error level message");
;
    }
}
