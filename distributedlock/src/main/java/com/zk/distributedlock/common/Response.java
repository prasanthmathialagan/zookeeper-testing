package com.zk.distributedlock.common;

/**
 * Created by prasanthmathialagan on 5/27/18.
 */
public enum Response {
    OK,
    ERROR,

    LOCK_OWNED_BY_ANOTHER_THREAD,
    LOCK_NOT_OWNED_BY_CURRENT_THREAD
}
