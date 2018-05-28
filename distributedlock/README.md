Procedure
---------

**Without failures:**
1. Run three zookeeper on localhost on the ports 2181, 2182, 2183 (These servers form the zookeeper ensemble)
2. Run ServerMain
3. Run several instances of ClientMain
4. All the clients should have owned the lock 5 times

**With failures (at most one failure)**
1. Follow steps 1-3
2. Kill/Restart one server at a time (Do it multiple times)
3. All the clients should have owned the lock 5 times in spite of failures