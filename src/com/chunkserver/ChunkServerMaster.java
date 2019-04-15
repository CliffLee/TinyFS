package com.chunkserver;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import com.client.Client;
import com.client.ClientFS;
import com.client.ClientFS.FSReturnVals;
import com.interfaces.ChunkServerMasterInterface;

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
		this.namespace = new TreeMap<String, List<String>>();
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
			configOut.println("localhost:" + servePort);
			configOut.close();
		} catch (IOException e) {
			System.out.println("ERR: Failed to open new server socket!");
			e.printStackTrace();
		}
		
		System.out.printf("Chunk Server Master running on port %d...\n", servePort);

		// accept connections from clients
		Socket conn = null;
		ObjectInputStream ois = null;
		ObjectOutputStream oos = null;

		while (true) {
			try {
				conn = serveSocket.accept();

				ois = new ObjectInputStream(conn.getInputStream());
				oos = new ObjectOutputStream(conn.getOutputStream());

				while (!conn.isClosed()) {
					int payloadSize = Client.ReadIntFromInputStream("ChunkServerMaster", ois);
					if (payloadSize == -1) break;

					int command = Client.ReadIntFromInputStream("ChunkServerMaster", ois);
					switch(command) {
					case ClientFS.CREATE_DIR_COMMAND:
						int srcLen = Client.ReadIntFromInputStream("ChunkServerMaster", ois);
						String src = new String(Client.RecvPayload("ChunkServerMaster", ois, srcLen));

						int destLen = Client.ReadIntFromInputStream("ChunkServerMaster", ois);
						String dest = new String(Client.RecvPayload("ChunkServerMaster", ois, destLen));

						oos.writeInt(FSReturnValsToOrdinalMiddleware(createDir(src, dest)));
						oos.flush();
						
						break;
					default:
						break;
					}
				}
			} catch (IOException e) {
				System.out.println("ERR: Failed to open new client socket!");
				e.printStackTrace();
			} finally {
				try {
					if (conn != null)
						conn.close();
					if (ois != null)
						ois.close();
					if (oos != null)
						oos.close();
				} catch (IOException e) {
					System.out.println("ERR: Failed to close client socket/resources!");
				}
			}
		}
	}

	/**
	 ***********
	 * FS API *
	 *********
	 */

	public FSReturnVals createDir(String src, String dirname) {
		// err if src dir not existent
		if (!dirExists(src)) {
			return FSReturnVals.SrcDirNotExistent;
		}

		// err if dest dir (full path) exists
		if (dirExists(src + dirname + "/")) {
			return FSReturnVals.DestDirExists;
		}

		// init join(src,dirname) to namespace
		addNamespaceEntry(src + dirname + "/", null);

		// do something that communicates over server to client Success
		return FSReturnVals.Success;
	}

	public FSReturnVals deleteDir(String src, String dirname) {
		// see if src dir exists
		if (!dirExists(src)) {
			return FSReturnVals.SrcDirNotExistent;
		}

		// see if dest dir exists
		if (!dirExists(String.join("/", src, dirname))) {
			return FSReturnVals.DestDirNotExistent;
		}

		// see if dest dir has children
		// only time target dir not empty is if size of list returned from util fn
		// is greater than 1
		if (findImmediateNamespaceDescendants(String.join("/", src, dirname)).size() > 1) {
			return FSReturnVals.DirNotEmpty;
		}

		// delete dest dir
		namespace.remove(String.join("/", src, dirname));
		
		// success
		return FSReturnVals.Success;
	}

	public FSReturnVals renameDir(String src, String newName) {
		// see if src dir exists
		if (!dirExists(src)) {
			return FSReturnVals.SrcDirNotExistent;
		}

		// see if new name exists already
		if (dirExists(newName)) {
			return FSReturnVals.DestDirExists;
		}

		// rename
		namespace.put(newName, namespace.get(src));
		namespace.remove(src);
		
		// success
		return FSReturnVals.Success;
	}

	public FSReturnVals listDir(String target, String[] result) {
		// see if src dir exists
		if (!dirExists(target)) {
			return FSReturnVals.SrcDirNotExistent;
		}

		// obtain immediate namespace descendant set
		// convert to array and populate result
		Set<String> resultSet = findImmediateNamespaceDescendants(target);
		result = resultSet.toArray(new String[resultSet.size()]);

		return FSReturnVals.Success;
	}

	/**
	 **********************
	 * FS Util Functions *
	 ********************
	 */
	private boolean dirExists(String path) {
		return
			this.namespace.containsKey(path)		// entry exists in namespace
			&& this.namespace.get(path) == null;		// entry is a directory
	}

	private boolean fileExists(String path) {
		return
			this.namespace.containsKey(path)		// entry exists in namespace
			&& this.namespace.get(path) != null			// entry is a file
			&& this.namespace.get(path).size() != 0;	// file chunk handle list is nonempty TODO CL: actually not sure if we should allow for non-zero size chunk handle lists
	}

	private void addNamespaceEntry(String path, List<String> chunkHandles) {
		if (chunkHandles == null) {
		// the entry is a directory
			namespace.put(path, null);
		} else {
		// the entry is a file
			namespace.put(path, new ArrayList<String>());
		}
	}

	private Set<String> findImmediateNamespaceDescendants(String prefix) {
		return namespace.keySet()
			.stream()
			.filter(s -> s.startsWith(prefix))
			.filter(s -> s.indexOf("/") == -1)
			.collect(Collectors.toSet());
	}

	private Set<String> findAllNamespaceDescendants(String prefix) {
		return namespace.keySet()
			.stream()
			.filter(s -> s.startsWith(prefix))
			.collect(Collectors.toSet());
	}

	private int FSReturnValsToOrdinalMiddleware(FSReturnVals val) {
		return val.ordinal();
	}

	/**
	 *********
	 * Main *
	 *******
	 */
	public static void main(String[] args) {
		ChunkServerMaster csm = new ChunkServerMaster();
		csm.serve();
	}
}
