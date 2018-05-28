package com.zk.distributedlock.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Created by prasanthmathialagan on 5/27/18.
 */
public class AcceptorThread extends Thread {
	private static final Logger LOG = LoggerFactory.getLogger(AcceptorThread.class);

	@Override
	public void run() {
		try {
			ServerSocket socket = new ServerSocket(30000);
			while (true) {
				Socket conn = socket.accept();
				new SocketIOThread(conn).start();
			}
		}
		catch (IOException e) {
			LOG.error("", e);
		}
	}
}
