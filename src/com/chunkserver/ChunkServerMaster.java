package com.chunkserver;

import com.client.Client;

/**
 * implementation of chunk server master
 * @author Clifford Lee
 * @author Sneha Patkar
 */

public class ChunkServerMaster implements ChunkServerMasterInterface {
	// CL: Networking info
	private String host;
	private int port;
	public final static String MasterConfigFile = "MasterConfig.txt";

	// CL: This should be a map of paths to potential chunk handle lists
	// CL: Thinking if List value is empty -> directory
	// CL:          else -> file
	private Map<String, List<String>> namespace;

	public ChunkServerMaster() {
		this.namespace = new HashMap<String, List<String>>();
	}

	public void serve() {
		// master server instantiated
		ChunkServerMaster master = new ChunkServerMaster();

		// port allocation and config writing
		int servePort = 0;
		ServerSocket serveSocket = null;
		try {
			serveSocket = new ServerSocket(servePort);
			servePort = serveSocket.getLocalPort();

			// if successful, write to config
			PrintWriter configOut = new PrintWriter(new FileOutputStream(MasterConfigFile));
			configOut.println("localhost:"+ServerPort);
			configOut.close();
		} catch (IOException e) {
			System.out.println("ERR: Failed to open new server socket!");
			e.printStackTrace();
		}

		Socket conn = null;
		while (true) {
			try {
				conn = 
			}
		}
	}

	public void createDir(String src, String dirname) {
		// find if src dir exists
		// find if join(src,dirname) exists
		// init join(src,dirname) to namespace
		// return true
	}
}
