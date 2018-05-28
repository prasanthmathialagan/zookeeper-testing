package com.zk.distributedlock.server;

import com.zk.distributedlock.common.Command;
import com.zk.distributedlock.common.Response;
import com.zk.distributedlock.common.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.zk.distributedlock.common.Utils.safeClose;
import static com.zk.distributedlock.common.Utils.safeWriteObject;

/**
 * Created by prasanthmathialagan on 5/27/18.
 */
public class SocketIOThread extends Thread {

    private static final Logger LOG = LoggerFactory.getLogger(SocketIOThread.class);
    private static final AtomicBoolean locked = new AtomicBoolean(false);
    private static final AtomicReference lockOwner = new AtomicReference();
    private static final AtomicLong counter = new AtomicLong(0);

    private final Socket socket;
    private final ObjectInputStream inputStream;
    private final ObjectOutputStream outputStream;
    private volatile boolean shutdown = false;
    private final long myId;

    public SocketIOThread(Socket socket) throws IOException {
        this.socket = socket;
        this.inputStream = new ObjectInputStream(socket.getInputStream());
        this.outputStream = new ObjectOutputStream(socket.getOutputStream());
        this.myId = counter.incrementAndGet();
    }

    @Override
    public void run() {
        LOG.info("Starting a new IO thread for socket from " + socket.toString());
        Thread.currentThread().setName(socket.toString());
        while (!shutdown) {
            Command command = Utils.safeReadObject(inputStream);
            LOG.debug("Command received = " + command);
            if (command == null) {
                safeWriteObject(outputStream, Response.ERROR);
            }

            switch (command) {
                case LOCK:
                    lock();
                    break;
                case UNLOCK:
                    unlock();
                    break;
                case QUIT:
                    cleanup();
                    safeWriteObject(outputStream, Response.OK);
                    break;
            }
        }

        LOG.info("Stopping IO thread for socket from " + socket.toString());

        // Clean up
        safeClose(inputStream);
        safeClose(outputStream);
        safeClose(socket);
    }

    private void unlock() {
        synchronized (locked) {
            if (locked.get()) {
                locked.set(false);
                safeWriteObject(outputStream, Response.OK);
                lockOwner.set(null);
                LOG.info(myId + " gives up the lock!!");
            } else {
                safeWriteObject(outputStream, Response.LOCK_NOT_OWNED_BY_CURRENT_THREAD);
                LOG.error("DANGER!! UNLOCK!!");
            }
        }
    }

    private void lock() {
        synchronized (locked) {
            if (locked.get()) { // Lock owned by another thread
                safeWriteObject(outputStream, Response.LOCK_OWNED_BY_ANOTHER_THREAD);
                LOG.error("DANGER!! LOCK");
            } else {
                locked.set(true);
                lockOwner.set(this);
                LOG.info(myId + " owns the lock!!");
                safeWriteObject(outputStream, Response.OK);
            }
        }
    }

    private void cleanup() {
        shutdown = true;
        synchronized (locked) {
            if (lockOwner.get() == this) {
                LOG.info("Cleaning up the mess!!!");
                locked.set(false);
                lockOwner.set(null);
            }
        }
    }
}
