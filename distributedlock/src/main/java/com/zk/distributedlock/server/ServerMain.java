package com.zk.distributedlock.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by prasanthmathialagan on 5/27/18.
 */
public class ServerMain {
    private static final Logger LOG = LoggerFactory.getLogger(ServerMain.class);

    public static void main(String[] args) {
        LOG.info("Starting the server process..");
        new AcceptorThread().start();
    }
}
