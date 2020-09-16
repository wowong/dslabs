package dslabs.clientserver;

import com.google.common.base.Objects;
import dslabs.clientserver.Reply;
import dslabs.clientserver.Request;
import dslabs.framework.Address;
import dslabs.framework.Client;
import dslabs.framework.Command;
import dslabs.framework.Node;
import dslabs.framework.Result;
import dslabs.kvstore.KVStore.KVStoreCommand;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import static dslabs.clientserver.ClientTimer.CLIENT_RETRY_MILLIS;

/**
 * Simple client that sends requests to a single server and returns responses.
 *
 * See the documentation of {@link Client} and {@link Node} for important
 * implementation notes.
 */
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
class SimpleClient extends Node implements Client {
    private final Address serverAddress;

	private Request request;
	private Reply reply;
	private int sequenceNum = 1;

    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    public SimpleClient(Address address, Address serverAddress) {
        super(address);
        this.serverAddress = serverAddress;
    }

    @Override
    public synchronized void init() {
        // No initialization necessary
    }

    /* -------------------------------------------------------------------------
        Client Methods
       -----------------------------------------------------------------------*/
    @Override
    public synchronized void sendCommand(Command command) {
        if (!(command instanceof KVStoreCommand)) {
            throw new IllegalArgumentException();
        }

        request = new Request(command, sequenceNum++);
        reply = null;

        send(request, serverAddress);
        set(new ClientTimer(request), CLIENT_RETRY_MILLIS);
    }

    @Override
    public synchronized boolean hasResult() {
        return reply != null;
    }

    @Override
    public synchronized Result getResult() throws InterruptedException {
		while (reply == null) {
			wait();
		}
		return reply.result();
    }

    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private synchronized void handleReply(Reply m, Address sender) {
        if (Objects.equal(m.sequenceNum(), request.sequenceNum())) {
            reply = m;
            notify();
        }
    }

    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    private synchronized void onClientTimer(ClientTimer t) {
        if (request != null && Objects.equal(t.request(), request) && reply == null) {
            request = new Request(request.command(), request.sequenceNum());
            send(request, serverAddress);
            set(t, CLIENT_RETRY_MILLIS);
        }
    }
}
