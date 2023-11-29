package server.faulttolerance;

import edu.umass.cs.nio.AbstractBytePacketDemultiplexer;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.nioutils.NodeConfigUtils;
import edu.umass.cs.utils.Util;
import server.ReplicatedServer;
import server.AVDBReplicatedServer.Type;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Level;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

import client.AVDBClient;
import client.MyDBClient;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.json.JSONObject;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.AddWatchMode;

import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.net.InetAddress;
import java.util.Collections;
import java.util.List;
import java.util.Collections;



/**
 * This class should implement your replicated fault-tolerant database server if
 * you wish to use Zookeeper or other custom consensus protocols to order client
 * requests.
 * <p>
 * Refer to {@link server.ReplicatedServer} for a starting point for how to do
 * server-server messaging or to {@link server.AVDBReplicatedServer} for a
 * non-fault-tolerant replicated server.
 * <p>
 * You can assume that a single *fault-tolerant* Zookeeper server at the default
 * host:port of localhost:2181 and you can use this service as you please in the
 * implementation of this class.
 * <p>
 * Make sure that both a single instance of Cassandra and a single Zookeeper
 * server are running on their default ports before testing.
 * <p>
 * You can not store in-memory information about request logs for more than
 * {@link #MAX_LOG_SIZE} requests.
 */
public class MyDBFaultTolerantServerZK extends server.MyDBSingleServer {

	/**
	 * Set this value to as small a value with which you can get tests to still
	 * pass. The lower it is, the faster your implementation is. Grader* will
	 * use this value provided it is no greater than its MAX_SLEEP limit.
	 */
	public static final int SLEEP = 1000;

	/**
	 * Set this to true if you want all tables drpped at the end of each run
	 * of tests by GraderFaultTolerance.
	 */
	public static final boolean DROP_TABLES_AFTER_TESTS=true;

	/**
	 * Maximum permitted size of any collection that is used to maintain
	 * request-specific state, i.e., you can not maintain state for more than
	 * MAX_LOG_SIZE requests (in memory or on disk). This constraint exists to
	 * ensure that your logs don't grow unbounded, which forces
	 * checkpointing to
	 * be implemented.
	 */
	public static final int MAX_LOG_SIZE = 400;

	public static final int DEFAULT_PORT = 2181;
	
	private static final Logger LOGGER = Logger.getLogger(MyDBFaultTolerantServerZK.class.getName());
	
	private final ZooKeeper zk;
	final private Session session;
    final private Cluster cluster;
    
    protected Integer last_executed_req_num = -1;
    protected final String myID;
    protected final MessageNIOTransport<String,String> serverMessenger;


	/**
	 * @param nodeConfig Server name/address configuration information read
	 *                      from
	 *                   conf/servers.properties.
	 * @param myID       The name of the keyspace to connect to, also the name
	 *                   of the server itself. You can not connect to any other
	 *                   keyspace if using Zookeeper.
	 * @param isaDB      The socket address of the backend datastore to which
	 *                   you need to establish a session.
	 * @throws IOException
	 */
	public MyDBFaultTolerantServerZK(NodeConfig<String> nodeConfig, String
			myID, InetSocketAddress isaDB) throws IOException {
		super(new InetSocketAddress(nodeConfig.getNodeAddress(myID),
				nodeConfig.getNodePort(myID) - ReplicatedServer
						.SERVER_PORT_OFFSET), isaDB, myID);
		session = (cluster=Cluster.builder().addContactPoint("127.0.0.1")
                .build()).connect(myID);
		log.log(Level.INFO, "Server {0} added cluster contact point", new Object[]{myID,});

		this.myID = myID;

		this.serverMessenger =  new MessageNIOTransport<String, String>(myID, nodeConfig, new AbstractBytePacketDemultiplexer() {
                            @Override
                            public boolean handleMessage(byte[] bytes, NIOHeader nioHeader) {
                                handleMessageFromServer(bytes, nioHeader);
                                return true;
                            }
                        }, true);

		log.log(Level.INFO, "Server {0} started on {1}", new Object[]{this.myID, this.clientMessenger.getListeningSocketAddress()});
		
		this.zk = new ZooKeeper("localhost:" + DEFAULT_PORT, 3000, null);
		
		try {
			// Create sequential ephemeral node under live_nodes
			String serverAddress = nodeConfig.getNodeAddress(myID) + ":" + nodeConfig.getNodePort(myID);
			this.zk.create("/live_nodes/" + this.myID, 
					serverAddress.getBytes(), 
					ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL_SEQUENTIAL);
			List<Integer> request_numbers = this.zk.getChildren("/requests",false).stream().map(Integer::valueOf).collect(Collectors.toList());
			
			// TODO: Make sure to do any needed crash recovery here.
			if (hasCheckpoint()) {
				//restore();
				//rollForward();		
			} else {
				
				
				Collections.sort(request_numbers);
				if (request_numbers.size() > 0 && request_numbers.get(0) == 0) {
//						roll_forward();
				} else {
					System.out.println("ERROR! Did not find checkpoint or initial logs");
				}
			}
			
			registerRequestsWatcher();
				
		}
		catch (KeeperException e) {
				e.printStackTrace();
		} catch (InterruptedException e) {
				e.printStackTrace();
		}
	}
	
	protected Boolean hasCheckpoint() {
		return false;
	}

	/**
	 * TODO: process bytes received from clients here.
	 */
	protected void handleMessageFromClient(byte[] bytes, NIOHeader header) {
		String request = new String(bytes);
		String primary = getPrimary();
		
		JSONObject packet = new JSONObject();
		packet.put(MyDBClient.Keys.REQUEST.toString(), request);
		packet.put(MyDBClient.Keys.TYPE.toString(), "CLIENT_UPDATE");
		
		this.serverMessenger.send(primary, packet.toString().getBytes());
	}

	/**
	 * TODO: process bytes received from fellow servers here.
	 */
	protected void handleMessageFromServer(byte[] bytes, NIOHeader header) {
		byte[] b = this.zk.getData("/requests",false, null);
	}


	/**
	 * TODO: Gracefully close any threads or messengers you created.
	 */
	public void close() {
		super.close();
		this.serverMessenger.stop();
		session.close();
		cluster.close();
		try {
            zk.close();
        } catch (InterruptedException e) {
        	LOGGER.log(Level.SEVERE, "Error closing ZooKeeper connection", e);
        }
	}

	public static enum CheckpointRecovery {
		CHECKPOINT, RESTORE;
	}
	
	public static void takeSnapshot(String keyspace, String snapshotName) throws IOException, InterruptedException {
        ProcessBuilder processBuilder = new ProcessBuilder("nodetool", "snapshot", "-t", snapshotName, keyspace);
        processBuilder.redirectErrorStream(true);

        Process process = processBuilder.start();
        process.waitFor();
        // You can optionally read the process output or wait for the process to finish
        // (e.g., using process.waitFor())
    }
	
	public static void restoreSnapshot(String keyspace, String snapshotName) throws IOException, InterruptedException {
        ProcessBuilder processBuilder = new ProcessBuilder("nodetool", "clearsnapshot", "-t", snapshotName, keyspace);
        processBuilder.redirectErrorStream(true);

        Process process = processBuilder.start();
        process.waitFor();
        // You can optionally read the process output or wait for the process to finish
        // (e.g., using process.waitFor())
    }
	
	public void checkPoint() throws IOException,InterruptedException {
		takeSnapshot(this.myID, "checkpoint_"+last_executed_req_num);
       
	}
	
	public void restore(String snapshotName) throws IOException,InterruptedException {
		restoreSnapshot(this.myID, snapshotName);
		rollForward();
	}

	public void trimLogs() {
	    try {
	        // Get the list of children from ZooKeeper
	        List<String> children = zk.getChildren("/requests", false);

	        // Check if there are at least 400 elements
	        if (children.size() >= 400) {
	            // Get the sublist of the first 100 elements
	            List<String> sublist = children.subList(0, 100);

	            // Remove the first 100 elements from the list
	            children.removeAll(sublist);

	            // Delete the corresponding nodes from ZooKeeper
	            for (String node : sublist) {
	                String path = "/requests/" + node;
	                zk.delete(path, -1);
	            }
	        }
	    } catch (KeeperException | InterruptedException e) {
	        e.printStackTrace();
	    }
	}
	
	public int rollForward() throws IOException {
	    try {
	        // Get the list of children from ZooKeeper
	        List<String> children = zk.getChildren("/requests", false);
	        
	        // Sort the children to ensure they are processed in order
	        Collections.sort(children);

	        // Flag to determine if the lastExecReq has been found
	        boolean foundLastExecReq = false;

	        // Iterate through the children
	        for (String node : children) {
	            int nodeInt = Integer.parseInt(node);

	            // Skip until we find the last executed request for that server
	            if (!foundLastExecReq && nodeInt != last_executed_req_num + 1) {
	                continue;
	            } else {
	                foundLastExecReq = true;
	            }

	            // Execute the request (replace this with your logic)
	            session.execute(new String(zk.getData("/requests" + node, null,null)));

	            // Update the lastExecReq
	            last_executed_req_num = nodeInt;
	        }
	        
	        if(last_executed_req_num % 50 == 0) {
	        	checkPoint();
	        }

	    } catch (KeeperException | InterruptedException e) {
	        e.printStackTrace();
	    }
	    return last_executed_req_num;
	}

	public InetSocketAddress getPrimary() {
	    List<String> children = null;
	    try {
	        // Get the list of children from ZooKeeper
	        children = zk.getChildren("/live-nodes", false);
	    } catch (KeeperException | InterruptedException e) {
	        e.printStackTrace();
	    }
	    // Check if the list is not null and not empty before accessing elements
	    if (children != null && !children.isEmpty()) {
	    	String[] parts = children.get(0).split(":");
        	InetSocketAddress primaryIP = new InetSocketAddress(parts[0], Integer.parseInt(parts[1]) - ReplicatedServer.SERVER_PORT_OFFSET);
	        return primaryIP;
	    } else {
	        // Handle the case where the list is null or empty
	        return null; // You can return a default value or handle it based on your requirements
	    }
	}

	private class RequestsWatcher implements Watcher {
        @Override
        public void process(WatchedEvent event) {
            if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
	                // New znode created in /requests
	            try {
            		System.out.println("New znode created in /requests");
	                rollForward();
	                List<String> children = zk.getChildren("/requests", false);
	                if(children.size() >= 400) {
	                	InetSocketAddress primary = getPrimary();
	                	serverMessenger.send(primary, ("Trim Logs").getBytes());

	                }
	            	registerRequestsWatcher();
            	}
            	catch (KeeperException | InterruptedException | IOException e) {
        	        e.printStackTrace();
        	    }
            }
        }
    }
	
	private void registerRequestsWatcher() {
        try {
            // Get children and set a watch
            List<String> children = zk.getChildren("/requests", new RequestsWatcher());
            System.out.println("Current children of /requests: " + children);
        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
        }
    }
	
	/**
	 * @param args args[0] must be server.properties file and args[1] must be
	 *             myID. The server prefix in the properties file must be
	 *             ReplicatedServer.SERVER_PREFIX. Optional args[2] if
	 *             specified
	 *             will be a socket address for the backend datastore.
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		new MyDBFaultTolerantServerZK(NodeConfigUtils.getNodeConfigFromFile
				(args[0], ReplicatedServer.SERVER_PREFIX, ReplicatedServer
						.SERVER_PORT_OFFSET), args[1], args.length > 2 ? Util
				.getInetSocketAddressFromString(args[2]) : new
				InetSocketAddress("localhost", 9042));
	}

}