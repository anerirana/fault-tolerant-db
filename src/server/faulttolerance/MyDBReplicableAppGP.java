package server.faulttolerance;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.exceptions.InvalidQueryException;

import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.nioutils.NodeConfigUtils;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.json.JSONObject;
import java.util.logging.Logger;



/**
 * This class should implement your {@link Replicable} database app if you wish
 * to use Gigapaxos.
 * <p>
 * Make sure that both a single instance of Cassandra is running at the default
 * port on localhost before testing.
 * <p>
 * Tips:
 * <p>
 * 1) No server-server communication is permitted or necessary as you are using
 * gigapaxos for all that.
 * <p>
 * 2) A {@link Replicable} must be agnostic to "myID" as it is a standalone
 * replication-agnostic application that via its {@link Replicable} interface is
 * being replicated by gigapaxos. However, in this assignment, we need myID as
 * each replica uses a different keyspace (because we are pretending different
 * replicas are like different keyspaces), so we use myID only for initiating
 * the connection to the backend data store.
 * <p>
 * 3) This class is never instantiated via a main method. You can have a main
 * method for your own testing purposes but it won't be invoked by any of
 * Grader's tests.
 */
public class MyDBReplicableAppGP implements Replicable {

	/**
	 * Set this value to as small a value with which you can get tests to still
	 * pass. The lower it is, the faster your implementation is. Grader* will
	 * use this value provided it is no greater than its MAX_SLEEP limit.
	 * Faster
	 * is not necessarily better, so don't sweat speed. Focus on safety.
	 */
	public static final int SLEEP = 1000;
	public Cluster cluster;
    public Session session;
    public String keyspace;
    private static final Logger logger = Logger.getLogger(MyDBReplicableAppGP.class.getName());
	/**
	 * All Gigapaxos apps must either support a no-args constructor or a
	 * constructor taking a String[] as the only argument. Gigapaxos relies on
	 * adherence to this policy in order to be able to reflectively construct
	 * customer application instances.
	 *
	 * @param args Singleton array whose args[0] specifies the keyspace in the
	 *             backend data store to which this server must connect.
	 *             Optional args[1] and args[2]
	 * @throws IOException
	 */
	public MyDBReplicableAppGP(String[] args) throws IOException {
		// TODO: setup connection to the data store and keyspace
        String keyspace = args[0];
        this.cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        this.session = cluster.connect(keyspace);
        //this.logger = logger;
        
	}

	/**
	 * Refer documentation of {@link Replicable#execute(Request, boolean)} to
	 * understand what the boolean flag means.
	 * <p>
	 * You can assume that all requests will be of type {@link
	 * edu.umass.cs.gigapaxos.paxospackets.RequestPacket}.
	 *
	 * @param request
	 * @param b
	 * @return
	 */
	@Override
	public boolean execute(Request request, boolean doNotReplyToClient) {
	    try {
	        String queryJson = request.toString();
	        JSONObject json = new JSONObject(queryJson);
	        String query = json.getString("QV");
	        session.execute(query);

	        if (!doNotReplyToClient) {
	            // TODO: Send response back to the client if needed
	            // The application is expected to either send a response back to the client
	            // or delegate response messaging to Paxos via the ClientRequest.getResponse() interface.
	        }
	        return true;
	    } catch (Exception e) {
	    	logger.info("Error executing request");
	        return false;
	    }
	}

	/**
	 * Refer documentation of
	 * {@link edu.umass.cs.gigapaxos.interfaces.Application#execute(Request)}
	 *
	 * @param request
	 * @return
	 */
	@Override
	public boolean execute(Request request) {
	    try {
	        String queryJson = request.toString();
	        JSONObject json = new JSONObject(queryJson);
	        String query = json.getString("QV");
	        session.execute(query);
	        return true;
	    } catch (Exception e) {
	    	logger.info("Error executing request");
	        return false;
	    }
	}

	protected void takeSnapshot(String snapshotName) throws IOException, InterruptedException {
		 String snapshotDirectory = "./checkpoints/";
		 File directory = new File(snapshotDirectory);

        if (!directory.exists()) {
        	directory.mkdirs();
        }
		
		ResultSet results = session.execute("SELECT * FROM grade");
		
		Map<Integer,ArrayList<Integer>> tables = new HashMap<Integer, ArrayList<Integer>>();
		for(Row row : results) {		
				int curKey = row.getInt(0);
				tables.put(curKey,new ArrayList<Integer>(row.getList(1,
						Integer.class)));
		}
		
		ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(snapshotDirectory + snapshotName + ".ser"));
		out.writeObject(tables);
		out.close();
	}

	protected boolean restoreSnapshot(String snapshotName) throws IOException, InterruptedException {
		logger.info("Restoring from snapshot: " + snapshotName);

	    String snapshotDirectory = "./checkpoints/";
	    String snapshotFilePath = snapshotDirectory + snapshotName + ".ser";

	    ObjectInputStream in = new ObjectInputStream(new FileInputStream(snapshotFilePath));

	    try {
	        Map<Integer, ArrayList<Integer>> tables = (Map<Integer, ArrayList<Integer>>) in.readObject();

	        for (Map.Entry<Integer, ArrayList<Integer>> entry : tables.entrySet()) {
	            Integer key = entry.getKey();
	            ArrayList<Integer> values = entry.getValue();

	            if (!values.isEmpty()) {
	                String query = "INSERT into grade (id, events) values (" + key + ", [";

	                query += values.get(0);

	                for (int i = 1; i < values.size(); i++) {
	                    query += "," + values.get(i);
	                }
	                query += "]);";
	                session.execute(query);
	            }
	        }
	        return true;
	    } catch (ClassNotFoundException | IOException e) {
	    	logger.info("Couldn't restore snapshots");
	        return false;
	    } finally {
	        in.close();
	    }
	}

	
	/**
	 * Refer documentation of {@link Replicable#checkpoint(String)}.
	 *
	 * @param s
	 * @return
	 */
	@Override
    public String checkpoint(String name) {
        // Create a snapshot of the Cassandra keyspace
		logger.info("Checkpoint creating:" +name);
        String snapshotName = "snapshot_" + name;
        try {
        	takeSnapshot(snapshotName);
        } catch (IOException | InterruptedException e) {
        	logger.info("Checkpoint Fail");
        }
        return snapshotName;
    }

    @Override
    public boolean restore(String name, String state) {
        // Restore Cassandra keyspace from the snapshot
        try {
            if(restoreSnapshot("snapshot_" + name)) {
            	logger.info("Restoring:" +name);
            	restoreSnapshot("snapshot_" + name);
            }
        } catch (IOException | InterruptedException e) {
        	logger.info("Restore Fail");
        }
        return true; // Restoration successful
    }

	/**
	 * No request types other than {@link edu.umass.cs.gigapaxos.paxospackets
	 * .RequestPacket will be used by Grader, so you don't need to implement
	 * this method.}
	 *
	 * @param s
	 * @return
	 * @throws RequestParseException
	 */
	@Override
	public Request getRequest(String s) throws RequestParseException {
		return null;
	}

	/**
	 * @return Return all integer packet types used by this application. For an
	 * example of how to define your own IntegerPacketType enum, refer {@link
	 * edu.umass.cs.reconfiguration.examples.AppRequest}. This method does not
	 * need to be implemented because the assignment Grader will only use
	 * {@link
	 * edu.umass.cs.gigapaxos.paxospackets.RequestPacket} packets.
	 */
	@Override
	public Set<IntegerPacketType> getRequestTypes() {
		return new HashSet<IntegerPacketType>();
	}
}
