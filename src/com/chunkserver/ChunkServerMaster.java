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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import com.client.ClientRec;
import com.client.ClientFS.FSReturnVals;
import com.client.FileHandle;

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
	
	// SP: This enables us to create new chunkhandles
	private int ChunkIndex = 1;

	// CL: This should be a map of paths to potential chunk handle lists
	// CL: Thinking if List value is empty -> directory
	// CL:          else -> file
	private Map<String, List<String>> namespace;

	// Thread executor pool
	public final ExecutorService threadPool = Executors.newFixedThreadPool(10);

	public ChunkServerMaster() {
		this.namespace = new TreeMap<String, List<String>>() {{
			put("/", null);
		}};
	}

	
	public void serve() {
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

		// main serve loop
		try {
			while (true) {
				threadPool.execute(new ChunkServerMasterThread(serveSocket.accept(), this));
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			threadPool.shutdown();
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
		if (!dirExists(src + dirname + "/")) {
			return FSReturnVals.DestDirNotExistent;
		}

		// see if dest dir has children
		// only time target dir not empty is if size of list returned from util fn
		// is greater than 1
		Set<String> descendants = findImmediateNamespaceDescendants(src + dirname + "/");
		if (descendants.size() > 0) {
			return FSReturnVals.DirNotEmpty;
		}

		// delete dest dir
		namespace.remove(src + dirname + "/");
		
		// success
		return FSReturnVals.Success;
	}

	public FSReturnVals renameDir(String src, String newName) {
		// see if src dir exists
		if (!dirExists(src + "/")) {
			return FSReturnVals.SrcDirNotExistent;
		}

		// see if new name exists already
		if (dirExists(newName + "/")) {
			return FSReturnVals.DestDirExists;
		}

		// rename
		namespace.put(newName + "/", namespace.get(src + "/"));
		namespace.remove(src + "/");
		
		// success
		return FSReturnVals.Success;
	}

	public FSReturnVals listDir(String target, List<String> result) {
		// see if src dir exists
		if (!dirExists(target)) {
			return FSReturnVals.SrcDirNotExistent;
		}

		// obtain immediate namespace descendant set
		// convert to array and populate result
		Set<String> resultSet = findAllNamespaceDescendants(target);
		for (String s : resultSet) {
			result.add(s);
		}
		
		return FSReturnVals.Success;
	}

	public FSReturnVals createFile(String parent, String filename) {
		// err if parent dir not existent
		if (!dirExists(parent)) {
			return FSReturnVals.SrcDirNotExistent;
		}

		// err if file exists
		if (fileExists(parent + filename)) {
			return FSReturnVals.FileExists;
		}

		// create file in namespace
		addNamespaceEntry(parent + filename, new ArrayList<String>());

		// TODO CL: store info about chunks associated with file?

		// return success
		return FSReturnVals.Success;
	}

	public FSReturnVals deleteFile(String parent, String filename) {
		// err if parent dir does not exist
		if (!dirExists(parent)) {
			return FSReturnVals.SrcDirNotExistent;
		}

		// err if file does not exist
		if (!fileExists(parent + filename)) {
			return FSReturnVals.FileDoesNotExist;
		}

		// remove file from namespace
		removeNamespaceEntry(parent + filename);

		// TODO CL: invalidate any chunks associated with this filename
		// return success
		return FSReturnVals.Success;
	}

	public FSReturnVals openFile(String filename, FileHandle fh) {
		// TODO CL: probably more lock stuff here
		fh.setFilePath(filename);
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
			&& this.namespace.get(path) != null;		// entry is a file
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

	private void removeNamespaceEntry(String path) {
		namespace.remove(path);
	}

	private Set<String> findImmediateNamespaceDescendants(String prefix) {
		int depth = (int) prefix.chars().filter(ch -> ch == '/').count();
		return namespace.keySet()
			.stream()
			.filter(s -> s.startsWith(prefix))
			.filter(s -> s.chars().filter(ch -> ch == '/').count() == depth + 1)
			.map(s -> s.substring(0, s.length() - 1))
			.collect(Collectors.toSet());
	}

	private Set<String> findAllNamespaceDescendants(String prefix) {
		int depth = (int) prefix.chars().filter(ch -> ch == '/').count();
		return namespace.keySet()
			.stream()
			.filter(s -> s.startsWith(prefix))
			.filter(s -> s.chars().filter(ch -> ch == '/').count() >= depth + 1)
			.map(s -> s.substring(0, s.length() - 1))
			.collect(Collectors.toSet());
	}
	
	// SP: Added for appendRecord functionality
	public String getLastChunk (String filepath) {
		String s = "None";
		if (this.namespace.containsKey(filepath)) {
			List<String> chunks = this.namespace.get(filepath);
			if (chunks.size() != 0)
			{
				return chunks.get(chunks.size()-1);
			}
		}
		return s;
	}

	public void reset() {
		// reset namespace
		namespace.clear();

		// reset chunk to file/address mappings
	}

	// SP: Added for appendRecord functionality
	public String addChunk(String filepath) {
		String newChunkhandle = null;
		if (this.namespace.containsKey(filepath)) {
			newChunkhandle = String.valueOf(ChunkIndex);
			this.namespace.get(filepath).add(newChunkhandle);
			ChunkIndex +=1;
		}
		return newChunkhandle;
	}
	
	// SP: Added for getFirstRecord functionality
	public String getChunk (String filepath, int chunkIndex) {
		//Invalid chunkIndex
		if (chunkIndex < -1)
		{
			return null;
		}
		String s = null;
		if (this.namespace.containsKey(filepath)) {
			List<String> chunks = this.namespace.get(filepath);
			if ((chunks.size() != 0) && chunkIndex < chunks.size())
			{
				return chunks.get(chunkIndex);
			}
			else
			{
				return null;
			}
		}
		return s;
	}

	// SP: Added for getNextRecord functionality
		public int getChunkIndex (String filepath, String chunkHandle) {
			if (this.namespace.containsKey(filepath))
			{
				List<String> chunks = this.namespace.get(filepath);
				return chunks.indexOf(chunkHandle);
				
			}
			return -1;
		}	
	
	
	// SP: Added for getFirstRecord functionality
	public int getNumChunks (String filepath) {
		if (this.namespace.containsKey(filepath)) {
			List<String> chunks = this.namespace.get(filepath);
			return chunks.size();
		}
		return -1;
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
