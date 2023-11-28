package client;

import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;
import org.json.JSONException;
import org.json.JSONObject;
import server.SingleServer;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class should implement your DB client.
 */
public class MyDBClient extends Client {
    private NodeConfig<String> nodeConfig= null;
    long reqnum = 0;
    ConcurrentHashMap<Long, Callback> callbacks = new ConcurrentHashMap<Long,
            Callback>();

    public MyDBClient(NodeConfig<String> nodeConfig) throws IOException {
        super();
        this.nodeConfig = nodeConfig;
    }

    /** TODO: This method will automatically get invoked whenever any response
     * is received from a remote node. You need to implement logic here to
     * ensure that whenever a response is received, the callback method
     * that was supplied in callbackSend(.) when the corresponding request
     * was sent is invoked.
     *
     * Extend this method in MyDBClient to implement your logic there. This
     * file will be overwritten to the original in your submission.
     *
     * @param bytes The content of the received response
     * @param header Sender and receiver information about the received response
     */
    protected void handleResponse(byte[] bytes, NIOHeader header) {
        // expect echo reply here
        try {
            JSONObject response = new JSONObject(new String(bytes, SingleServer
                    .DEFAULT_ENCODING));
            Callback callback = callbacks.get(response.getLong(Keys
                    .REQNUM.toString()));
            if(callback!=null)
                callback.handleResponse(bytes, header);

        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (JSONException e) {
            //e.printStackTrace();
        }
    }

    public static enum Keys {
        REQNUM, TYPE, REQUEST, RESPONSE, CLOCK, HEADER;
    }
    private synchronized long getUniqueIdentifier(String request) {
        return reqnum++;
    }

    /**
     * TODO: This method, unlike the simple send above, should invoke the
     * supplied callback's handleResponse method upon receiving the
     * corresponding response from the remote end.
     *
     * Extend this method in MyDBClient to implement your logic there. This
     * file will be overwritten to the original in your submission.
     *
     * @param isa
     * @param request
     * @param callback
     */
    public void callbackSend(InetSocketAddress isa, String request,
                             Callback callback) throws IOException {
        try {
            JSONObject json = new JSONObject().put(Keys.REQNUM.toString(),
                    getUniqueIdentifier(request)).put(Keys.REQUEST.toString(), request);
            this.callbacks.put(json.getLong(Keys.REQNUM.toString()), callback);
            this.send(isa, json.toString());
        } catch (JSONException e) {
            e.printStackTrace();
        }
    }
}