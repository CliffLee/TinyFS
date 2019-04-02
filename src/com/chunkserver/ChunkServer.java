package com.chunkserver;

import com.interfaces.ChunkServerInterface;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * implementation of interfaces at the chunkserver side
 * 
 * @author Clifford Lee 9477802367
 *
 */

public class ChunkServer implements ChunkServerInterface {
	// networking
	public String host;
	public int port; 
	public ServerSocket csSocket;
	
	// client thread pool
	public final ExecutorService threadPool = Executors.newFixedThreadPool(10);

	// db/file management
	final static String filePath = "C:\\Users\\shahram\\Documents\\TinyFS-2\\csci485Disk\\"; // or C:\\newfile.txt
//	final static String filePath = "C:\\Users\\leecliff\\Documents\\TinyFS-2\\csci485Disk\\"; // or C:\\newfile.txt
//	final static String filePath = "\\\\Vlabfs.vlab.usc.edu\\home$\\leecliff\\Desktop\\cs485\\TinyFS\\csci485Disk\\";
//	final static String filePath = "/Volumes/SD/edu/cs485/project/temp/";

	public static long counter;

	public static void main(String[] args) {
		ChunkServer cs = new ChunkServer();
		
		cs.serve();
	}
	
	/**
	 * ChunkServer constructor
	 */
	public ChunkServer() {
		try {
			this.initialize();
			this.generateConfigFile();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 *******************
	 * Networking API *
	 *****************
	 */
	
	/**
	 * Server initialization function
	 */
	private void initialize() throws IOException {
		// initialize static counter to number of files in ChunkServer dir
		try {
			ChunkServer.counter = new File(filePath).list().length;
		} catch (NullPointerException e) {
				e.printStackTrace();
		}
		
		// attempt to assign network variables and initialize server vars
		this.host = InetAddress.getLocalHost().getHostAddress();
		this.port = 5432;

		// initialize chunk server ServerSocket
		this.csSocket = new ServerSocket(port);
		
		// register shutdown hook for threadpool
		this.registerHooks();
	}
	
	
	/**
	 * Register hooks
	 * Primarily for shutdown hook on SIGINT for graceful shutdown
	 */
	private void registerHooks() {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				threadPool.shutdown();
			}
		});		
	}
	
	/**
	 * Config file generation function
	 * 
	 * Creates config file for clients to use once server is done initializing
	 */
	public void generateConfigFile() {
		File config = new File("./config.txt");
		String configString = String.format("%s:%d", this.host, this.port);
		FileWriter configWriter = null;
		try {
			configWriter = new FileWriter(config, false);
			configWriter.write(configString);
			configWriter.close();
			
			System.out.printf("Configuration written to \'%s\'\n", config.getAbsolutePath());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Server execution loop function
	 */
	public void serve() {
		System.out.printf("ChunkServer serving at %s:%d...\n", this.host, this.port);
		
		try {
			for (;;) {
				this.threadPool.execute(new ChunkServerThread(this.csSocket.accept(), this));
			}
		} catch (IOException e) {
			e.printStackTrace();
			this.threadPool.shutdown();
		}

		if (this.csSocket != null) {
			try {
				this.csSocket.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Health check function
	 */
	public String healthcheck() {
		System.out.println("Server pinged for health check");
		
		return "STATUS OK";
	}
	
	
	/**
	 ****************
	 * FileSys API *
	 **************
	 */

	/**
	 * Each chunk corresponds to a file. Return the chunk handle of the last chunk
	 * in the file.
	 */
	public String initializeChunk() {
		// initialize as a null string
		String chunkHandle = null;
		RandomAccessFile chunkFile = null;
		
		try {
			// generate a valid chunk handle
			chunkHandle = String.format("%s_%d", InetAddress.getLocalHost().getHostAddress(), counter);
			++counter;

			// create a chunk file
			chunkFile = new RandomAccessFile(filePath + chunkHandle, "rw");
			chunkFile.setLength(ChunkServer.ChunkSize);
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		} finally {
			try {
				chunkFile.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		return chunkHandle;
	}

	/**
	 * Write the byte array to the chunk at the specified offset The byte array size
	 * should be no greater than 4KB
	 */
	public boolean putChunk(String ChunkHandle, byte[] payload, int offset) {
		RandomAccessFile chunk = null;
		
		try {
			// check if chunk handle is valid & file exists
			File chunkFile = new File(filePath + ChunkHandle);
			if (!chunkFile.exists()) {
				return false;
			}

			// load file with chunk handle
			chunk = new RandomAccessFile(chunkFile, "rw");

			// write payload at offset for given length of payload
			chunk.write(payload, offset, payload.length);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (chunk != null) {
				try {
					chunk.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		return true;
	}

	/**
	 * read the chunk at the specific offset
	 */
	public byte[] getChunk(String ChunkHandle, int offset, int NumberOfBytes) {
		RandomAccessFile chunk = null;
		try {
			chunk = new RandomAccessFile(filePath + ChunkHandle, "r");
			
			byte[] buffer = new byte[NumberOfBytes];
			chunk.read(buffer, offset, NumberOfBytes);
			
			return buffer;
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				chunk.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		return null;
	}

}
