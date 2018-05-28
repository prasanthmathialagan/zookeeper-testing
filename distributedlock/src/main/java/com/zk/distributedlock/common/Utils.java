package com.zk.distributedlock.common;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;

/**
 * Created by prasanthmathialagan on 5/27/18.
 */
public class Utils {
    private static final Logger LOG = LoggerFactory.getLogger(Utils.class);
    public static final int MAX_RETRIES = 5;

    public static <T> T safeReadObject(ObjectInputStream o) {
        try {
            return (T) o.readObject();
        } catch (IOException e) {
            LOG.error("", e);
        } catch (ClassNotFoundException e) {
            LOG.error("", e);
        }

        return null;
    }

    public static boolean safeWriteObject(ObjectOutputStream os, Object o) {
        try {
            os.writeObject(o);
            return true;
        } catch (IOException e) {
            LOG.error("", e);
            return false;
        }
    }

    public static void sleep(long time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
        }
    }

    /**
     * It retries in case of CONNECTION LOSS
     *
     * @return Returns -1 instead of throwing exception
     */
    public static int safeGetMinimumChildIndex(ZooKeeper zk, String parentPath, String childPath) {
        List<String> children = null;
        int i = 0;
        for (; i < MAX_RETRIES; i++) {
            try {
                children = zk.getChildren(parentPath, true);
                break;
            } catch (KeeperException e) {
                if (e.code() != KeeperException.Code.CONNECTIONLOSS) {
                    LOG.error("", e);
                    break;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        if (children == null || children.isEmpty()) {
            if (i == MAX_RETRIES) {
                LOG.error("Cannot get children for node " + parentPath + " even after " + MAX_RETRIES + " attempts. So abandoning!!");
            }
            return -1;
        }

        int min = Integer.MAX_VALUE;
        for (String p : children) {
            int n = Integer.parseInt(p.substring(childPath.length()));
            if (min > n) {
                min = n;
            }
        }

        return min;
    }

    /**
     * It retries in case of CONNECTION LOSS
     *
     * @return Returns null instead of throwing exception
     */
    public static String safeCreateSequentialEphemeralNode(ZooKeeper zk, String path) {
        for (int i = 0; i < MAX_RETRIES; i++) {
            try {
                return zk.create(path, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            } catch (KeeperException e) {
                if (e.code() != KeeperException.Code.CONNECTIONLOSS) {
                    LOG.error("", e);
                    return null;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        LOG.error("Cannot create sequential ephemeral node " + path + " even after " + MAX_RETRIES + " attempts. So abandoning!!");
        return null;
    }

    public static void safeWait(Object o) {
        synchronized (o) {
            try {
                o.wait();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * It retries in case of CONNECTION LOSS
     */
    public static void safeDelete(ZooKeeper zk, String path) {
        for (int i = 0; i < MAX_RETRIES; i++) {
            try {
                zk.delete(path, 0);
                return;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (KeeperException e) {
                if (e.code() != KeeperException.Code.CONNECTIONLOSS) {
                    LOG.error("", e);
                    return;
                }
            }
        }

        LOG.error("Cannot delete node " + path + " even after " + MAX_RETRIES + " attempts. So abandoning!!");
    }

    public static void safeClose(Closeable closeable) {
        if (closeable == null) {
            return;
        }

        try {
            closeable.close();
        } catch (IOException e) {
            LOG.error("", e);
        }
    }

    public static void safeCloseZk(ZooKeeper zk) {
        try {
            zk.close();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
