package server.faulttolerance;

import edu.umass.cs.nio.AbstractBytePacketDemultiplexer;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.nioutils.NodeConfigUtils;
import edu.umass.cs.utils.Util;
import server.ReplicatedServer;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
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
import org.json.JSONException;
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
import java.io.File;
import java.util.ArrayList;



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
	public static final String DEFAULT_ENCODING = "ISO-8859-1";
	
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
		
		
		this.serverMessenger =  new
                MessageNIOTransport<String, String>(myID, nodeConfig,
                new
                        AbstractBytePacketDemultiplexer() {
                            @Override
                            public boolean handleMessage(byte[] bytes, NIOHeader nioHeader) {
                                handleMessageFromServer(bytes, nioHeader);
                                return true;
                            }
                        }, true);

		log.log(Level.INFO, "Server {0} started on {1}", new Object[]{this.myID, this.clientMessenger.getListeningSocketAddress()});
		
		//Integer.toString(nodeConfig.getNodePort(myID)- ReplicatedServer.SERVER_PORT_OFFSET)
		this.zk = new ZooKeeper("localhost:" + DEFAULT_PORT, 
				3000, 
				new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (event.getType() == Watcher.Event.EventType.NodeCreated) {
                	System.out.println("Children data changed.");
                }
            }
        });
		
		try {
			// Create sequential ephemeral node under live_nodes
			String serverAddress = nodeConfig.getNodeAddress(myID) + ":" + nodeConfig.getNodePort(myID);
			System.out.println(serverAddress);
			this.zk.create("/live_nodes/" + this.myID, 
					serverAddress.getBytes(DEFAULT_ENCODING), 
					ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL);
			List<String> request_numbers = this.zk.getChildren("/requests",false);
			Collections.sort(request_numbers, Collections.reverseOrder());
			// TODO: Make sure to do any needed crash recovery here.
			if (hasCheckpoint()) {
				restoreLatestSnapshot();
				rollForward();		
			} else {
				if (request_numbers.size() > 0) {
					if (Integer.valueOf(request_numbers.get(0)) == 0)
						rollForward();
					else
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
		// Specify the directory where snapshots are stored
        String snapshotDirectory = "/var/lib/cassandra/data/" + this.myID + "/snapshots";

        // Check if any snapshot is available
        return getAvailableSnapshots(snapshotDirectory).size() > 0;
	}

	/**
	 * TODO: process bytes received from clients here.
	 * @throws InterruptedException 
	 * @throws KeeperException 
	 */
	@Override
	protected void handleMessageFromClient(byte[] bytes, NIOHeader header) {
		try {
		String request = new String(bytes);
		InetSocketAddress primary = getPrimary();
			
		JSONObject packet = new JSONObject();
		packet.put(MyDBClient.Keys.REQUEST.toString(), request);
		packet.put(MyDBClient.Keys.TYPE.toString(), "CLIENT_UPDATE");
		
		this.serverMessenger.send(primary, packet.toString().getBytes());
		}
		catch(JSONException | IOException | InterruptedException | KeeperException e) {
			e.printStackTrace();
		}
	}

	/**
	 * TODO: process bytes received from fellow servers here.
	 */
	protected void handleMessageFromServer(byte[] bytes, NIOHeader header) {
		try {
			JSONObject json = new JSONObject(new String(bytes));
			String request = json.getString(AVDBClient.Keys.REQUEST.toString()); 
			String requestType = json.getString(AVDBClient.Keys.TYPE.toString());
					

			if (requestType.equals("CLIENT_UPDATE")) {
				
			} else if (requestType.equals("TRIM_LOGS")) {
				trimLogs();
			}
			this.zk.create("/requests/", 
					request.getBytes(), 
					ZooDefs.Ids.OPEN_ACL_UNSAFE,
	                CreateMode.PERSISTENT_SEQUENTIAL);
        } catch (KeeperException | JSONException | InterruptedException e) {
            e.printStackTrace();
        }
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
	
	protected void takeSnapshot(String keyspace, String snapshotName) throws IOException, InterruptedException {
        ProcessBuilder processBuilder = new ProcessBuilder("nodetool", "snapshot", "-t", snapshotName, keyspace);
        processBuilder.redirectErrorStream(true);

        Process process = processBuilder.start();
        process.waitFor();
        // You can optionally read the process output or wait for the process to finish
        // (e.g., using process.waitFor())
    }
	
	protected void restoreSnapshot(String snapshotName) throws IOException, InterruptedException {
        ProcessBuilder processBuilder = new ProcessBuilder("nodetool", "clearsnapshot", "-t", snapshotName, this.myID);
        processBuilder.redirectErrorStream(true);

        Process process = processBuilder.start();
        process.waitFor();
        // You can optionally read the process output or wait for the process to finish
        // (e.g., using process.waitFor())
    }
	
	protected void restoreLatestSnapshot() throws IOException, InterruptedException {
        // Specify the directory where snapshots are stored
        String snapshotDirectory = "/var/lib/cassandra/data/" + this.myID + "/snapshots";

        // Get a list of available snapshots
        List<String> snapshotNames = getAvailableSnapshots(snapshotDirectory);

        // Restore the latest snapshot (assuming the list is sorted)
        if (!snapshotNames.isEmpty()) {
            String latestSnapshot = snapshotNames.get(snapshotNames.size() - 1);
            this.last_executed_req_num = Integer.valueOf(latestSnapshot.split("_")[0]);
            restoreSnapshot(latestSnapshot);
        } else {
            System.out.println("No snapshots found for keyspace: " + this.myID);
        }
    }

	protected List<String> getAvailableSnapshots(String snapshotDirectory) {
        List<String> snapshotNames = new ArrayList<>();

        // Create a File object for the snapshot directory
        File directory = new File(snapshotDirectory);

        // Check if the directory exists
        if (directory.exists() && directory.isDirectory()) {
            // List the contents of the directory (snapshot names)
            String[] snapshots = directory.list();

            // Add each snapshot name to the list
            if (snapshots != null) {
                for (String snapshot : snapshots) {
                    snapshotNames.add(snapshot);
                }
            }
        }
        return snapshotNames;
    }
	
	protected void checkPoint() throws IOException,InterruptedException {
		takeSnapshot(this.myID, "checkpoint_"+last_executed_req_num);
       
	}
	
	protected void restore(String snapshotName) throws IOException,InterruptedException {
		restoreSnapshot(snapshotName);
		rollForward();
	}

	protected void trimLogs() {
	    try {
	        // Get the list of children from ZooKeeper
	        List<String> children = zk.getChildren("/requests", false);
	        Collections.sort(children, Collections.reverseOrder());
	        // Check if there are at least 400 elements
	        if (children.size() >= 400) {
	            // Get the sublist of the first 100 elements
	            List<String> sublist = children.subList(0, 50);

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
	
	protected int rollForward() throws IOException {
	    try {
	        // Get the list of children from ZooKeeper
	        List<String> children = zk.getChildren("/requests", false);
	        Collections.sort(children, Collections.reverseOrder());
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

	protected InetSocketAddress getPrimary() throws KeeperException, InterruptedException {
	    try {
	        // Get the list of children from ZooKeeper
	    	 List<String> children = zk.getChildren("/live_nodes", false);

		    // Check if the list is not null and not empty before accessing elements
		    if (children != null && !children.isEmpty()) {
		    	
		    	String[] parts = new String(this.zk.getData("/live_nodes/" + this.myID, null, null), DEFAULT_ENCODING).split(":");
	        	InetSocketAddress primaryIP = new InetSocketAddress(parts[0].split("/")[0], Integer.parseInt(parts[1]));
		        return primaryIP;
		    } else {
		        // Handle the case where the list is null or empty
		        return null; // You can return a default value or handle it based on your requirements
		    }
	    } catch (KeeperException | InterruptedException | UnsupportedEncodingException e) {
	        e.printStackTrace();
	    }
	    return null;
	}

	protected class RequestsWatcher implements Watcher {
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
	                	JSONObject packet = new JSONObject();
	            		packet.put(MyDBClient.Keys.TYPE.toString(), "TRIM_LOGS");
	            		serverMessenger.send(primary, packet.toString().getBytes());
	                }
	            	registerRequestsWatcher();
            	}
            	catch (KeeperException | InterruptedException | IOException | JSONException e) {
        	        e.printStackTrace();
        	    }
            }
        }
    }
	
	protected void registerRequestsWatcher() {
        try {
            // Get children and set a watch
            List<String> children = zk.getChildren("/requests", new RequestsWatcher());
            Collections.sort(children, Collections.reverseOrder());
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
		new MyDBFaultTolerantServerZK(NodeConfigUtils.getNodeConfigFromFile(args[0], ReplicatedServer.SERVER_PREFIX, ReplicatedServer.SERVER_PORT_OFFSET), 
				args[1], 
				args.length > 2 ? Util.getInetSocketAddressFromString(args[2]) : new InetSocketAddress("localhost", 9042));
	}

}