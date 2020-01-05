package de.uni_stuttgart.ipvs.ids.communication;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Vector;

/**
 * Part b) Extend the method receiveMessages to return all DatagramPackets that
 * were received during the given timeout.
 * 
 * Also implement unpack() to conveniently convert a Collection of
 * DatagramPackets containing ValueResponseMessages to a collection of
 * VersionedValueWithSource objects.
 * 
 */
public class NonBlockingReceiver {

	protected DatagramSocket socket;

	public NonBlockingReceiver(DatagramSocket socket) {
		this.socket = socket;
	}

	public Vector<DatagramPacket> receiveMessages(int timeoutMillis, int expectedMessages)
			throws IOException {
		// TODO: Implement me!
		boolean keepGoing = true;
		int count =0;
		Vector<DatagramPacket>received = new Vector<DatagramPacket>();
		while(keepGoing)
		{
		    try {
		        byte[] data = new byte[2000];  
		        DatagramPacket packet = new DatagramPacket(data, data.length);  
		        socket.receive(packet);  
		         
		         
		        //if the packet is empty or null, then the server is done sending?
		        if ( count >= expectedMessages )
		            keepGoing = false;
		        else 
		        	received.add(packet);
		    }
		    catch(SocketTimeoutException ste)
		    {
		        //if we haven't received anything, than return error
		        if ( count > 0 )
		            return received;
		        else 
		        	return null;
		    }
		    finally {
		        //socket.close();
		    }
		}
		 return received;
	}

	public static <T> Collection<MessageWithSource<T>> unpack(
			Collection<DatagramPacket> packetCollection) throws IOException,
			ClassNotFoundException {
		// TODO: Impelement me!	
		
		Collection<MessageWithSource<T>> MessageList = new ArrayList<MessageWithSource<T>>();
		
		Iterator iter = packetCollection.iterator();
		while(iter.hasNext())
		{
			
			MessageWithSource<T> msgWithSource = (MessageWithSource<T>)iter.next();
			MessageList.add(msgWithSource);
		}
		
		
		return null;
	}
	
}
