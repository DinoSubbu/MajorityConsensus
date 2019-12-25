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

import de.uni_stuttgart.ipvs.ids.communication.ReadRequestMessage;
import de.uni_stuttgart.ipvs.ids.communication.ReleaseReadLock;
import de.uni_stuttgart.ipvs.ids.communication.ReleaseWriteLock;
import de.uni_stuttgart.ipvs.ids.communication.RequestReadVote;
import de.uni_stuttgart.ipvs.ids.communication.RequestWriteVote;
import de.uni_stuttgart.ipvs.ids.communication.ValueResponseMessage;
import de.uni_stuttgart.ipvs.ids.communication.Vote;
import de.uni_stuttgart.ipvs.ids.communication.WriteRequestMessage;

public class Replica<T> extends Thread {

	public enum LockType {
		UNLOCKED, READLOCK, WRITELOCK
	};

	private int id;

	private double availability;
	private VersionedValue<T> value;

	protected DatagramSocket socket = null;
	
	protected LockType lock;
	
	/**
	 * This address holds the address of the client holding the lock. This
	 * variable should be set to NULL every time the lock is set to UNLOCKED.
	 */
	protected SocketAddress lockHolder;

	public Replica(int id, int listenPort, double availability, T initialValue) throws SocketException {
		super("Replica:" + listenPort);
		this.id = id;
		SocketAddress socketAddress = new InetSocketAddress("127.0.0.1", listenPort);
		this.socket = new DatagramSocket(socketAddress); // UDP Socket
		this.availability = availability;
		this.value = new VersionedValue<T>(0, initialValue);
		this.lock = LockType.UNLOCKED;
	}
	

	/**
	 * Part a) Implement this run method to receive and process request
	 * messages. To simulate a replica that is sometimes unavailable, it should
	 * randomly discard requests as long as it is not locked.
	 * The probability for discarding a request is (1 - availability).
	 * 
	 * For each request received, it must also be checked whether the request is valid.
	 * For example:
	 * - Does the requesting client hold the correct lock?
	 * - Is the replica unlocked when a new lock is requested?
	 */
	public void run() {
		// TODO: Implement me!
		// Message reception and processing via UDP
		// Use availability value to discard packets
		// Upon request, lock and after doing task, unlock it.
		try {
			int discardCounterMax = 8; // Because in the test case, check happens for 10 tries.
			int noMsgToDiscard =   (int) ( discardCounterMax * (1 - this.availability) );
			int discardMsgMarker = discardCounterMax - noMsgToDiscard;
			int msgCounter = 1;
			while(true)
			{
				byte[] data = new byte[10000]; // TODO: Check Buffer size needed !! 
				DatagramPacket message = new DatagramPacket(data, data.length);
				socket.receive(message);
				String messageType = message.getClass().getName();

				if ( (msgCounter > discardMsgMarker) && msgCounter <= discardCounterMax) {
				// Discard those messages
					msgCounter = msgCounter + 1;
					// Don't send any response to the received message. Node Unavailability
					continue;
				}
				else if (messageType.contains("ReadRequestMessage"))
				{
					if( (lock == LockType.UNLOCKED) || 
							( (lock == LockType.READLOCK) && (lockHolder.equals(message.getSocketAddress())) ) )
					{
						if (lock == LockType.UNLOCKED) {
							lock = LockType.READLOCK; // Obtain Read Lock
							lockHolder = message.getSocketAddress(); // Who is the current Lock owner??
						}
						// Create Object for message class ValueResponseMessage
						ValueResponseMessage<T> valueResponseMsg = new ValueResponseMessage<T>(this.value.getValue());
						
						ByteArrayOutputStream baos = new ByteArrayOutputStream();
						// create Output Stream object of type ByteArray
					    ObjectOutputStream oos = new ObjectOutputStream(baos);
					    oos.writeObject(valueResponseMsg); // Write msg object to ObjectOutputStream
					    
					    byte[] responseMsg = baos.toByteArray(); // Retrieve byte array buffer from baos
					    // Create Data gram Packet and send response to lockholder
					    DatagramPacket response = new DatagramPacket(responseMsg, responseMsg.length, lockHolder);
					    socket.send(response);
					    
					    // Release Lock before exiting.
					    lock = LockType.UNLOCKED;
					    lockHolder = null;
					}
					else {
						// Send NACK for the request
						sendVote(message.getSocketAddress(), Vote.State.NACK, this.value.getVersion());
					}
				}
				else if (messageType.contains("ReleaseReadLock"))
				{
					lock = LockType.UNLOCKED;
					lockHolder = null;
					// Send ACK for the request
					sendVote(lockHolder, Vote.State.ACK, this.value.getVersion());
				}
				else if (messageType.contains("ReleaseWriteLock"))
				{
					lock = LockType.UNLOCKED;
					lockHolder = null;
					// Send ACK for the request
					sendVote(lockHolder, Vote.State.ACK, this.value.getVersion());
				}
				else if (messageType.contains("RequestReadVote"))
				{
					if (lock != LockType.WRITELOCK) // TODO: Check this logic again!
					{
						lock = LockType.READLOCK;
						lockHolder = message.getSocketAddress(); // Who is the current Lock owner??
						
						// Send positive Vote
						sendVote(lockHolder, Vote.State.YES, this.value.getVersion());
					}
					else
					{
						// Send Negative Vote // sTODO: Check if version is needed for NO Vote
						sendVote(lockHolder, Vote.State.NO, this.value.getVersion());
					}
					
				}
				else if (messageType.contains("RequestWriteVote"))
				{
					if (lock == LockType.UNLOCKED) 
					{
						lock = LockType.WRITELOCK;
						lockHolder = message.getSocketAddress(); // Who is the current Lock owner??
						
						// Send positive Vote
						sendVote(lockHolder, Vote.State.YES, this.value.getVersion());
					}
					else
					{
						// Send Negative Vote // TODO: Check if version is needed for NO Vote
						sendVote(lockHolder, Vote.State.NO, this.value.getVersion());
					}
				}
				else if (messageType.contains("WriteRequestMessage"))
				{
					if( (lock == LockType.UNLOCKED) || 
							( (lock == LockType.WRITELOCK) && (lockHolder.equals(message.getSocketAddress())) ) )
					{
						if (lock == LockType.UNLOCKED) {
							lock = LockType.WRITELOCK; // Obtain Write Lock
							lockHolder = message.getSocketAddress(); // Who is the current Lock owner??
						}
						
						WriteRequestMessage<T> writeReqObject = (WriteRequestMessage<T>) getObjectFromMessage(data);
						
						// Check if the received update is valid
						if( writeReqObject.getVersion() > this.value.getVersion() )
						{										    
							// Update Contents of Replica -- Added Setters. Check if needed
							this.value.setValue(writeReqObject.getValue());
							this.value.setVersion(writeReqObject.getVersion());
							// Send ACK for the request
							sendVote(lockHolder, Vote.State.ACK, this.value.getVersion());
						}
						else 
						{
							// Send NACK for the request
							sendVote(lockHolder, Vote.State.NACK, this.value.getVersion());
						}
					}
				}
				else 
				{	
					System.out.println("Invalid Message Format. Ignoring the message");	
					// Send NACK for the request
					sendVote(message.getSocketAddress(), Vote.State.NACK, this.value.getVersion());
				}
				
				// Reset Message Counter to 1, after processing discardCounterMax no of messages
				if(msgCounter > discardCounterMax) {
					msgCounter = 1;
				}
			}
				
			} catch (IOException | ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * This is a helper method. You can implement it if you want to use it or just ignore it.
	 * Its purpose is to send a Vote (YES/NO depending on the state) to the given address.
	 */
	protected void sendVote(SocketAddress address,
			Vote.State state, int version) throws IOException {
		// TODO: Implement me!
		
		// Create Object for message class Vote
		Vote voteMsg = new Vote(state, version);
		
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		// create Output Stream object of type ByteArray
	    ObjectOutputStream oos = new ObjectOutputStream(baos);
	    oos.writeObject(voteMsg); // Write msg object to ObjectOutputStream
	    
	    byte[] responseMsg = baos.toByteArray(); // Retrieve byte array buffer from baos
	    
	    // Create DataGram Packet and send vote to requester
	    DatagramPacket vote = new DatagramPacket(responseMsg, responseMsg.length, address);
	    socket.send(vote);
	}

	/**
	 * This is a helper method. You can implement it if you want to use it or just ignore it.
	 * Its purpose is to extract the object stored in a DatagramPacket.
	 */
	protected Object getObjectFromMessage(byte[] buffer)
			throws IOException, ClassNotFoundException {
		// TODO: Implement me!
		// From the buffer, reconstruct data object.
		ByteArrayInputStream bais = new ByteArrayInputStream(buffer); // bais - byte array input stream
	    ObjectInputStream message = new ObjectInputStream(bais); // create Input Stream object of type Byte
	    return message.readObject();
	}

	public int getID() {
		return id;
	}

	public SocketAddress getSocketAddress() {
		return socket.getLocalSocketAddress();
	}

}

