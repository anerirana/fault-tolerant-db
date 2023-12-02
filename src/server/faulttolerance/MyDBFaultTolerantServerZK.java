package server.faulttolerance;

import edu.umass.cs.nio.AbstractBytePacketDemultiplexer;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.nioutils.NodeConfigUtils;
import edu.umass.cs.utils.Util;
import server.ReplicatedServer;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Collections;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.ArrayList;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;



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
    protected Integer checkpoint_counter = 0;
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
			// Crash recovery here.		
			if (hasCheckpoint()) {
				restoreLatestSnapshot();
				rollForward();		
			} else {	
				List<String> request_numbers = this.zk.getChildren("/requests",false);
				Collections.sort(request_numbers);						
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
        String snapshotDirectory = "./checkpoints/" + this.myID + "/";

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
		this.zk.create("/requests/", 
				request.getBytes(), 
				ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT_SEQUENTIAL);		
		}
		catch(InterruptedException | KeeperException e) {
			e.printStackTrace();
		}
	}

	/**
	 * TODO: process bytes received from fellow servers here.
	 */
	protected void handleMessageFromServer(byte[] bytes, NIOHeader header) {
		
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
            LOGGER.log(Level.INFO, "zookeper connection closed");
        } catch (InterruptedException e) {
        	LOGGER.log(Level.SEVERE, "Error closing ZooKeeper connection", e);
        }
	}

	public static enum CheckpointRecovery {
		CHECKPOINT, RESTORE;
	}
	
	protected void takeSnapshot(String keyspace, String snapshotName) throws IOException, InterruptedException {
		 String snapshotDirectory = "./checkpoints/" + this.myID + "/";
		 File directory = new File(snapshotDirectory);

        //  Create directory if doesn't exist
        if (!directory.exists()) {
        	directory.mkdirs();
        } 
		 
        // Get current state of the table
		ResultSet results = session.execute("SELECT * FROM grade");		
		Map<Integer,ArrayList<Integer>> tables = new HashMap<Integer, ArrayList<Integer>>();
		for(Row row : results) {		
				int curKey = row.getInt(0);
				tables.put(curKey,new ArrayList<Integer>(row.getList(1,Integer.class)));
		}	
		
		// Get the name of last snapshot taken
        List<String> snapshotNames = getAvailableSnapshots(snapshotDirectory);
        File lastSnapshot = null;
        if (snapshotNames.size() > 0) {
        	lastSnapshot = new File(snapshotDirectory + snapshotNames.get(0)); 
        }
        
		// Serialize and save table object in file
		ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(snapshotDirectory + snapshotName + ".ser"));
		out.writeObject(tables);
		out.close();

		// delete the last snapshot
		// only keep latest snapshots
		if (lastSnapshot != null) {
			lastSnapshot.delete();
		}
				
		trimLogs();
	}

	protected void restoreSnapshot(String snapshotName) throws IOException, InterruptedException {
		ObjectInputStream in = new ObjectInputStream(new FileInputStream(snapshotName));
		try {	
			// Read table state from file
			Map<Integer,ArrayList<Integer>> tables = (Map<Integer, ArrayList<Integer>>) in.readObject();
			
			// Execute query to insert saved table state
			for (Map.Entry<Integer, ArrayList<Integer>> entry : tables.entrySet()) {
			    Integer key = entry.getKey();
			    ArrayList<Integer> values = entry.getValue();
			    System.out.println("restoring key: " + key + " values: " + values);
			    String query = "INSERT into grade (id, events) values (" + Integer.toString(key) + ", [";
			    if (values.size() > 0) {
			    	query += Integer.toString(values.get(0));
				    for (int i = 1; i < values.size(); i++) {
				    	query += "," + Integer.toString(values.get(i));
				    }
			    }
			    query += "]);";
			    session.execute(query);
			}		
		} catch (ClassNotFoundException e) {
			LOGGER.log(Level.SEVERE, "Error restoring snapshot!!\n Unrecognizable file format", e);		
		}
		in.close();	
	}

	
	protected void restoreLatestSnapshot() throws IOException, InterruptedException {
        // Specify the directory where snapshots are stored
        String snapshotDirectory = "./checkpoints/" + this.myID  + "/";;

        // Get a list of available snapshots
        List<String> snapshotNames = getAvailableSnapshots(snapshotDirectory);

        // Restore the latest snapshot (assuming only the most recent one is stored)
        if (!snapshotNames.isEmpty()) {
            String latestSnapshot = snapshotNames.get(0);
            this.last_executed_req_num = Integer.valueOf(latestSnapshot.split("\\.")[0].split("_")[1]);
            restoreSnapshot(snapshotDirectory + latestSnapshot);
        } else {
            LOGGER.log(Level.WARNING, "No snapshots found for keyspace:" + this.myID);	
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
		// checkpoint with the last executed request number
		takeSnapshot(this.myID, "checkpoint_" +last_executed_req_num);     
	}
	
	protected void restore(String snapshotName) throws IOException,InterruptedException {
		restoreSnapshot(snapshotName);
		rollForward();
	}

	protected void trimLogs() {
	    try {
	        // Get the list of children from ZooKeeper
	        List<String> children = zk.getChildren("/requests", false);
	        Collections.sort(children);
	        // Check if there are at least 400 elements
	        if (children.size() >= 400) {
	            // Get the sublist of the first 100 elements
	            List<String> sublist = children.subList(0, 50);

	            // Delete 100 request nodes from ZooKeeper
	            for (String node : sublist) {
	                String path = "/requests/" + node;
	                zk.delete(path, -1);
	            }
	        }
	    } catch (KeeperException | InterruptedException e) {
	    	LOGGER.log(Level.SEVERE, "Delete request node unsuccessful", e);	
	    }
	}
	
	protected int rollForward() throws IOException {
	    try {
	        // Get the list of children from ZooKeeper
	        List<String> children = zk.getChildren("/requests", false);
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

	            // Execute the request
	            session.execute(new String(zk.getData("/requests/" + node, null,null)));        

	            // Update the lastExecReq
	            last_executed_req_num = nodeInt;
	            incrementCheckpointCounter();
		        if(checkpoint_counter % 50 == 0) {
		        	resetCheckpointCounter();
		        	checkPoint();
		        }
	        }
	    } catch (KeeperException | InterruptedException e) {
	    	LOGGER.log(Level.SEVERE, "Unable to execute pending requests");
	    }
	    return last_executed_req_num;
	}
	
	protected synchronized void incrementCheckpointCounter() {
		++checkpoint_counter;
	}
	
	protected synchronized void resetCheckpointCounter() {
		checkpoint_counter = 0;
	}

	protected class RequestsWatcher implements Watcher {
        @Override
        public void process(WatchedEvent event) {
        	registerRequestsWatcher();
            if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
	                // New znode created in /requests
            	try {
					rollForward();
				} catch (IOException e) {
					e.printStackTrace();
				}
            }
        }
    }
	
	protected void registerRequestsWatcher() {
        try {
            // Get children and set a watch
            List<String> children = zk.getChildren("/requests", new RequestsWatcher());
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