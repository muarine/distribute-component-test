package com.rtmap.example.zookeeper.client;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * com.rtmap.example.zookeeper.client.TestZnode
 *
 * @author Muarine<maoyun@rtmap.com>
 * @date 16/6/30
 * @since 1.0
 */
public class TestZnode {


    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {


        ZooKeeper zookeeper = new ZooKeeper("10.10.10.114:2181", 1000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println("监控到新的消息输出:" + event.toString());
            }
        } , true);


//        List<String> children = zookeeper.getChildren("/", new Watcher() {
//            @Override
//            public void process(WatchedEvent event) {
//                System.out.println(event.toString());
//            }
//        });

//        recurZnode(zookeeper , "/");


        for (int i = 0; i < 10 ; i++) {
            Long counter = zookeeperCounter(zookeeper , "/yangxia/distribute/counter");
            System.out.println("成功计数:" + counter);
        }


    }

    private static final String PREFIX = "/";

    public static void recurZnode(ZooKeeper zooKeeper , String path){

        if(zooKeeper == null) {
            System.out.println("zookeeper object must not be null.");
            return;
        }

        try {
            List<String> children = zooKeeper.getChildren(path , true);
            if (children == null || children.isEmpty()){
                return;
            }
            // /config/topics/RT-SINK_KAFKA
            for (String s:
                 children) {
                String path2 = path.equals("/") ? path + s : path + PREFIX + s;
                recurZnode(zooKeeper , path2);
                System.out.println(path2);
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    public static Long zookeeperCounter(final ZooKeeper zooKeeper ,final String path) throws KeeperException, InterruptedException {
        Long counter = 1L;
        Stat stat = zooKeeper.exists(path , true);
        if(stat == null){
//            zooKeeper.create(path,"1".getBytes() , ZooDefs.Ids.OPEN_ACL_UNSAFE , CreateMode.PERSISTENT);
            createZnodes(zooKeeper , path);
        }else{
            byte[] bytes = zooKeeper.getData(path, true , null);
            counter = Long.valueOf(new String(bytes));
            counter++;
            zooKeeper.setData(path , counter.toString().getBytes() , -1);
        }
        return counter;
    }

    /**
     * /rtmap/distribute/counter
     *
     * @param path
     */
    public static void createZnodes(final ZooKeeper zooKeeper , String path) throws KeeperException, InterruptedException {

        if(path == null || path.trim().equals("")){
            throw new RuntimeException("the path must not be empty.");
        }
        if (path.equals("/")){
            throw new RuntimeException("the path is invalid that have no need to creating.");
        }
        if(!path.startsWith("/")){
            throw new RuntimeException("the path must start with '/'.");
        }
        if(path.endsWith("/")){
            throw new RuntimeException("the path must not end with '/'.");
        }
        String SEPARATER = "/";
        String[] ss = path.split("/");
        List<String> paths = new ArrayList<String>();
        String PREFIX = SEPARATER;
        for (String s:
             ss) {
            if(!s.trim().equals("")){
                PREFIX = PREFIX.equals(SEPARATER) ? PREFIX + s : PREFIX + SEPARATER + s;
                paths.add(PREFIX);
            }
        }

        for (int i = 0; i < paths.size(); i++) {
            if(i == paths.size() -1){
                zooKeeper.create(paths.get(i) , "1".getBytes() , ZooDefs.Ids.OPEN_ACL_UNSAFE , CreateMode.PERSISTENT);
            }else{
                zooKeeper.create(paths.get(i) , null , ZooDefs.Ids.OPEN_ACL_UNSAFE , CreateMode.PERSISTENT);
            }
        }
    }

}
