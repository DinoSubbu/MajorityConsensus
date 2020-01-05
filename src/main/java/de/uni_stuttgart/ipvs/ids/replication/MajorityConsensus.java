package de.uni_stuttgart.ipvs.ids.replication;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import de.uni_stuttgart.ipvs.ids.communication.MessageWithSource;
import de.uni_stuttgart.ipvs.ids.communication.NonBlockingReceiver;
import de.uni_stuttgart.ipvs.ids.communication.ReadRequestMessage;
import de.uni_stuttgart.ipvs.ids.communication.ReleaseReadLock;
import de.uni_stuttgart.ipvs.ids.communication.ReleaseWriteLock;
import de.uni_stuttgart.ipvs.ids.communication.RequestReadVote;
import de.uni_stuttgart.ipvs.ids.communication.RequestWriteVote;
import de.uni_stuttgart.ipvs.ids.communication.ValueResponseMessage;
import de.uni_stuttgart.ipvs.ids.communication.Vote;
import de.uni_stuttgart.ipvs.ids.communication.Vote.State;
import de.uni_stuttgart.ipvs.ids.communication.WriteRequestMessage;

public class MajorityConsensus<T> {

	protected Collection<SocketAddress> replicas;

	protected DatagramSocket socket;
	protected NonBlockingReceiver nbio;

	final static int TIMEOUT = 1000;

	public MajorityConsensus(Collection<SocketAddress> replicas, int port)
			throws SocketException {
		this.replicas = replicas;
		SocketAddress address = new InetSocketAddress("127.0.0.1", port);
		this.socket = new DatagramSocket(address);
		this.nbio = new NonBlockingReceiver(socket);
	}

	/**
	 * Part c) Implement this method.
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 */
	protected Collection<MessageWithSource<Vote>> requestReadVote() throws QuorumNotReachedException {
		// TODO: Implement me!
		// Request read votes from all replicas until quorum is reached
		// If not, throw Exception
		Collection<MessageWithSource<Vote>> readVotes = new ArrayList<MessageWithSource<Vote>>();
		Iterator iter = replicas.iterator();
		while(iter.hasNext())
		{
			RequestReadVote reqReadVote = new RequestReadVote();
			SocketAddress address = (SocketAddress) iter.next();
			byte[] data = new byte[10000]; // TODO: Check Buffer size needed !! 
			DatagramPacket message = new DatagramPacket(data, data.length);
			Vote vote = null;
			    		     
			try {
				
			sendUDPPacket(reqReadVote, address);
			
			System.out.println("sending to"+String.valueOf(address.toString()));
			
			socket.receive(message);
			//String msg = new String(message.getData(), message.getOffset(), message.getLength());
			//System.out.println(msg);
			ByteArrayInputStream bais = new ByteArrayInputStream(message.getData()); // bais - byte array input stream
		    ObjectInputStream is = new ObjectInputStream(bais); // create Input Stream object of type Byte
		
		    //System.out.println("vote:"+is.readObject().toString());
		    	vote = (Vote) is.readObject();
		    	
		    	
			} catch (ClassNotFoundException | IOException e) {
				// TODO Auto-generated catch block
				
				e.printStackTrace();
			}
			System.out.println("suc");
		    MessageWithSource<Vote> msgWithSource = new MessageWithSource<Vote>(message.getSocketAddress(),vote);
		    readVotes.add(msgWithSource);
	
		}
		return readVotes;
	}
	
	/**
	 * Part c) Implement this method.
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 */
	protected void releaseReadLock(Collection<SocketAddress> lockedReplicas) throws IOException, ClassNotFoundException {
		// TODO: Implement me!
		Iterator iter = lockedReplicas.iterator();
		
		while(iter.hasNext())
		{
			ReleaseReadLock releaseReadLockMsg = new ReleaseReadLock();
			byte[] data = new byte[10000]; // TODO: Check Buffer size needed !! 
			DatagramPacket message = new DatagramPacket(data, data.length);
			
			SocketAddress address = (SocketAddress) iter.next();
			sendUDPPacket(releaseReadLockMsg, address);
			socket.receive(message);
			
			ByteArrayInputStream bais = new ByteArrayInputStream(message.getData()); // bais - byte array input stream
		    ObjectInputStream is = new ObjectInputStream(bais); // create Input Stream object of type Byte
		    Vote vote = (Vote) is.readObject();
		    
		    if (vote.getState() == Vote.State.ACK) 
		    {
		    	System.out.println("Released Lock");
				
		    }
			//TODO: Check if it's needed to read ACK message and retry unlock, if failed
		}
	}
	
	/**
	 * Part d) Implement this method.
	 */
	protected Collection<MessageWithSource<Vote>> requestWriteVote() throws QuorumNotReachedException {
		// TODO: Implement me!
		return null;
	}
	
	/**
	 * Part d) Implement this method.
	 */
	protected void releaseWriteLock(Collection<SocketAddress> lockedReplicas) {
		// TODO: Implement me!
	}
	
	/**
	 * Part c) Implement this method.
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 */
	protected T readReplica(SocketAddress replica)  {
		// TODO: Implement me!
		ReadRequestMessage readReqMsg = new ReadRequestMessage();
		byte[] data = new byte[10000]; // TODO: Check Buffer size needed !! 
		DatagramPacket message = new DatagramPacket(data, data.length);
		ValueResponseMessage<T> valueResponse=null;
		try {
		sendUDPPacket(readReqMsg, replica);
		socket.receive(message);
		
		ByteArrayInputStream bais = new ByteArrayInputStream(message.getData()); // bais - byte array input stream
	    ObjectInputStream is = new ObjectInputStream(bais); // create Input Stream object of type Byte
	    
		
			valueResponse = (ValueResponseMessage<T>) is.readObject();
		} catch (ClassNotFoundException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    
		return valueResponse.getValue();		
	}
	
	/**
	 * Part d) Implement this method.
	 */
	protected void writeReplicas(Collection<SocketAddress> lockedReplicas, VersionedValue<T> newValue) {
		// TODO: Implement me!
	}
	
	/**
	 * Part c) Implement this method (and checkQuorum(), see below) to read the
	 * replicated value using the majority consensus protocol.
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 */
	public VersionedValue<T> get() throws QuorumNotReachedException {
		// TODO: Implement me!
		int versionMax = 0;
		int versionCurr;
		
		Collection<MessageWithSource<Vote>> readVoteReplies, positiveReplies;
		Collection<SocketAddress> locksToRelease = new ArrayList<SocketAddress>();
		
		readVoteReplies = this.requestReadVote();
		positiveReplies = this.checkQuorum(readVoteReplies);
		SocketAddress replicaLatestValue = null;
		
		Iterator iter = positiveReplies.iterator();
		while(iter.hasNext())
		{
			MessageWithSource<Vote> replica = (MessageWithSource<Vote>)iter.next();
			locksToRelease.add(replica.getSource());
			versionCurr = replica.getMessage().getVersion();
			
			if (versionCurr > versionMax) {
				replicaLatestValue = replica.getSource();
				versionMax = versionCurr;
			}	
		}
		System.out.println(versionMax);
		T valueLatest = this.readReplica(replicaLatestValue);
		VersionedValue<T> valueWithVersion = new VersionedValue<T>(versionMax, valueLatest);
		try {
			this.releaseReadLock(locksToRelease);
		} catch (ClassNotFoundException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return valueWithVersion;
	}

	/**
	 * Part d) Implement this method to set the
	 * replicated value using the majority consensus protocol.
	 */
	public void set(T value) throws QuorumNotReachedException {
		// TODO: Implement me!
	}

	/**
	 * Part c) Implement this method to check whether a sufficient number of
	 * replies were received. If a sufficient number was received, this method
	 * should return the {@link MessageWithSource}s of the locked {@link Replica}s.
	 * Otherwise, a QuorumNotReachedException must be thrown.
	 * @throws QuorumNotReachedException 
	 */
	protected Collection<MessageWithSource<Vote>> checkQuorum(
			Collection<MessageWithSource<Vote>> replies) throws QuorumNotReachedException {
		// TODO: Implement me!
		
		int quorumMinSize = (int) ( (this.replicas.size()/2) + 1) ;
		Collection<SocketAddress> positiveRepliesAddr = new ArrayList<SocketAddress>();
		Collection<MessageWithSource<Vote>> positiveRepliesMsg = new ArrayList<MessageWithSource<Vote>>();
		Iterator iter = replies.iterator();
		
		while(iter.hasNext())
		{
			MessageWithSource<Vote> vote = (MessageWithSource<Vote>) iter.next();
			if (vote.getMessage().getState() == Vote.State.YES) 
		    {
				positiveRepliesAddr.add(vote.getSource());
				positiveRepliesMsg.add(vote);
		    }
		}
		System.out.println(positiveRepliesMsg.size());
		
		if (positiveRepliesMsg.size() > quorumMinSize)
		{
			return positiveRepliesMsg;
		}
		else
		{
			throw new QuorumNotReachedException(quorumMinSize,positiveRepliesAddr);
		}
		
	}
	
	// Helper Method to send UDP Packets
	public <MsgType> void sendUDPPacket(MsgType msgToSend, SocketAddress address) throws IOException
	{
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		// create Output Stream object of type ByteArray
	    ObjectOutputStream oos = new ObjectOutputStream(baos);
	    oos.writeObject(msgToSend); // Write message object to ObjectOutputStream
	    
	    byte[] msgInByteArray = baos.toByteArray(); // Retrieve byte array buffer from baos
	    
	    // Create DataGram Packet and send message to destination address
	    DatagramPacket vote = new DatagramPacket(msgInByteArray, msgInByteArray.length, address);
	    
		this.socket.send(vote);
		
	}
	protected Object getObjectFromMessage(byte[] buffer)
			throws IOException, ClassNotFoundException {
		// TODO: Implement me!
		// From the buffer, reconstruct data object.
		ByteArrayInputStream bais = new ByteArrayInputStream(buffer); // bais - byte array input stream
	    ObjectInputStream message = new ObjectInputStream(bais); // create Input Stream object of type Byte
	    return message.readObject();
	}

}
