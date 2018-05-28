package com.zk.distributedlock.client;

import com.zk.distributedlock.common.Command;
import com.zk.distributedlock.common.Response;
import com.zk.distributedlock.common.Utils;
import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Objects;

import static com.zk.distributedlock.common.Utils.*;

/**
 * Created by prasanthmathialagan on 5/27/18.
 */
public class ClientMain implements Runnable, Watcher {
    private static final Logger LOG = LoggerFactory.getLogger(ClientMain.class);

    public static void main(String[] args) throws InterruptedException, IOException, KeeperException {
        new ClientMain().run();
    }

    private final ZooKeeper zk;
    private final Socket socket;
    private final Object lock = new Object();

    // This assumes that there will be a node /lock
    public ClientMain() throws IOException, KeeperException, InterruptedException {
        this.zk = new ZooKeeper("localhost:2181,localhost:2182,localhost:2183", 4000, this);
        this.socket = new Socket("localhost", 30000);
    }

    @Override
    public void run() {
        ObjectOutputStream oos;
        ObjectInputStream ois = null;
        try {
            oos = new ObjectOutputStream(socket.getOutputStream());
        } catch (IOException e) {
            LOG.error("", e);
            return;
        }

        for (int i = 0; i < 5 ; i++) {

            // Create an ephemeral node in zookeeper to acquire the lock
            String path = safeCreateSequentialEphemeralNode(zk, "/lock/test");
            Objects.requireNonNull(path);
            int number = Integer.parseInt(path.substring("/lock/test".length()));

            // You don't own the lock. So wait. While is necessary to avoid spurious wake ups
            synchronized (lock) {
                while (true) {
                    // Don't worry about herd effect
                    int minimum = safeGetMinimumChildIndex(zk, "/lock", "/lock/test");
                    if (number == minimum) { // Got the lock
                        break;
                    }

                    safeWait(lock);
                    LOG.debug("Back from wait");
                }
            }

            if(!safeWriteObject(oos, Command.LOCK)) {
                // In case of a failure during LOCK, just quit so that you will let others get the lock
                break;
            }

            if (ois == null) {
                try {
                    ois = new ObjectInputStream(socket.getInputStream());
                } catch (IOException e) {
                    LOG.error("", e);
                    break;
                }
            }

            Response response = safeReadObject(ois);
            if (safeDeleteForNonOKResponse(path, response)) {
                continue; // It is okay to continue
            }

            LOG.info("I own the lock " + (i + 1) + " times!!");
            Utils.sleep(3000);

            if (!safeWriteObject(oos, Command.UNLOCK)) {
                break;
            }

            response = safeReadObject(ois);
            if (safeDeleteForNonOKResponse(path, response)) {
                continue; // It is okay to continue
            }

            // If unlock is successful, delete the ephemeral node so that others can compete for the lock
            safeDelete(zk, path);
        }

        // Ephemeral nodes will be deleted automatically here. No need to do safe delete
        safeWriteObject(oos, Command.QUIT);
        safeReadObject(ois); // No need to worry about the response here

        // Final clean up
        safeClose(ois);
        safeClose(oos);
        safeClose(socket);
        safeCloseZk(zk);
    }

    private boolean safeDeleteForNonOKResponse(String path, Response response) {
        if (response != Response.OK) { // This should not happen!!
            LOG.info("Non OK response obtained = " + response);
            safeDelete(zk, path);
            return true;
        }
        return false;
    }

    @Override
    public void process(WatchedEvent event) {
        LOG.debug("Event received " + event);

        Event.EventType type = event.getType();
        if (Event.EventType.NodeChildrenChanged == type) {
            synchronized (lock) {
                LOG.debug("Notifying!!");
                lock.notifyAll();
            }
        }
    }
}
