package com.rtmap.example.zookeeper.client;

/**
 * com.rtmap.example.zookeeper.client.DataMonitorListener
 *
 * @author Muarine<maoyun@rtmap.com>
 * @date 16/6/23
 * @since 1.0
 */
public interface DataMonitorListener {

    /**
     * The existence status of the node has changed.
     */
    void exists(byte data[]);

    /**
     * The ZooKeeper session is no longer valid.
     *
     * @param rc
     * the ZooKeeper reason code
     */
    void closing(int rc);

}
