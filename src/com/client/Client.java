package com.client;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import com.chunkserver.ChunkServer;
import com.interfaces.ClientInterface;

/**
 * implementation of interfaces at the client side
 * 
 * @author Clifford Lee 9477802367
 *
 */
public class Client implements ClientInterface {
	// networking
	public String host;
	public int port;
	public Socket csSocket;
	
	// command constants
	public static final int INIT_CHUNK = 0;
	public static final int PUT_CHUNK = 1;
	public static final int GET_CHUNK = 2;

	/**
	 * Initialize the client
	 */
	public Client() {
		// connect to server
		try {
			File configFile = new File("./config.txt");
			BufferedReader configReader = new BufferedReader(new FileReader(configFile));
			String[] config = configReader.readLine().split(":");
			configReader.close();

			this.host = config[0];
			this.port = Integer.parseInt(config[1]);
			this.csSocket = new Socket(host, port);
			
			System.out.printf("Initialized new client to %s:%d\n", host, port);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	// BROKEN: DO NOT USE
	public String healthcheck() {
		try {
			OutputStream out = this.csSocket.getOutputStream();
			InputStream in = this.csSocket.getInputStream();

			write(out, "healthcheck".getBytes());
			System.out.println(new String(read(in), StandardCharsets.UTF_8));
		} catch (IOException e) {
			e.printStackTrace();
		}		

		return "STATUS NOT OK";
	}

	/**
	 * Create a chunk at the chunk server from the client side.
	 * 
	 * command 0
	 */
	public String initializeChunk() {
		try {
			OutputStream out = this.csSocket.getOutputStream();
			InputStream in = this.csSocket.getInputStream();

			byte[] payload = ByteBuffer.allocate(4).putInt(INIT_CHUNK).array();
			write(out, payload);

			byte[] response = read(in);
			if (response == null) {
				return null;
			}
			
			return new String(response);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * Write a chunk at the chunk server from the client side.
	 * 
	 * command 1
	 */
	public boolean putChunk(String ChunkHandle, byte[] data, int offset) {
		try {
			OutputStream out = this.csSocket.getOutputStream();
			InputStream in = this.csSocket.getInputStream();
			
			byte[] chunkHandle = ChunkHandle.getBytes();
			
			List<byte[]> payload = Arrays.asList(
					ByteBuffer.allocate(4).putInt(PUT_CHUNK).array(),
					ByteBuffer.allocate(4).putInt(offset).array(),
					ByteBuffer.allocate(4).putInt(data.length).array(),
					data,
					ByteBuffer.allocate(4).putInt(chunkHandle.length).array(),
					chunkHandle
					);
			write(out, this.concatByteArrays(payload));

			byte[] response = read(in);
			
			return ByteBuffer.wrap(response).getInt() != 0;
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return false;
	}

	/**
	 * Read a chunk at the chunk server from the client side.
	 * 
	 * command 2
	 */
	public byte[] getChunk(String ChunkHandle, int offset, int NumberOfBytes) {
		if (NumberOfBytes + offset > ChunkServer.ChunkSize) {
			System.out.println("The chunk read should be within the range of the file, invalide chunk read!");
			return null;
		}
		
		try {
			InputStream in = this.csSocket.getInputStream();
			OutputStream out = this.csSocket.getOutputStream();
			
			byte[] chunkHandle = ChunkHandle.getBytes();
			
			List<byte[]> payload = Arrays.asList(
					ByteBuffer.allocate(4).putInt(GET_CHUNK).array(),
					ByteBuffer.allocate(4).putInt(offset).array(),
					ByteBuffer.allocate(4).putInt(NumberOfBytes).array(),
					ByteBuffer.allocate(4).putInt(chunkHandle.length).array(),
					chunkHandle
					);
			
			write(out, this.concatByteArrays(payload));
			
			byte[] response = read(in);
			return response;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	private byte[] concatByteArrays(List<byte[]> bufs) {
		ByteArrayOutputStream bytes = null;

		try {
			bytes = new ByteArrayOutputStream();
		
			for (byte[] buf : bufs) {
				bytes.write(buf);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return bytes.toByteArray();
	}
	
	private void write(OutputStream out, byte[] bytes) {
		try {
			out.write(
					this.concatByteArrays(
						Arrays.asList(
							ByteBuffer.allocate(4).putInt(bytes.length + 4).array(),
							bytes
						)
					)
				);
			out.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private byte[] read(InputStream in) {
		byte[] payload = null;
		try {
			byte[] payloadSize = new byte[4];
			if (in.read(payloadSize, 0, 4) != 4) {
				return null;
			}
			
			int size = ByteBuffer.wrap(payloadSize).getInt();
			payload = new byte[size - 4];
			if (in.read(payload, 0, size - 4) != size - 4) {
				return null;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return payload;
	}
}
