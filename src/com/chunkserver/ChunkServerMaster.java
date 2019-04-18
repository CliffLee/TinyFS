package com.chunkserver;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import com.client.Client;
import com.client.ClientFS;
import com.client.ClientFS.FSReturnVals;
import com.client.ClientRec;
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
	private int ChunkIndex = 0;

	// CL: This should be a map of paths to potential chunk handle lists
	// CL: Thinking if List value is empty -> directory
	// CL:          else -> file
	private Map<String, List<String>> namespace;

	public ChunkServerMaster() {
		this.namespace = new TreeMap<String, List<String>>() {{
			put("/", null);
		}};
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
					// handle initial payload size and command before mux
					int payloadSize = Client.ReadIntFromInputStream("ChunkServerMaster", ois);
					if (payloadSize == -1) break;

					int command = Client.ReadIntFromInputStream("ChunkServerMaster", ois);

					// mux switch
					switch(command) {
					case ClientFS.CREATE_DIR_COMMAND:
						// req format: <srcLen - srcBytes - destLen - destBytes>
						int srcLen1 = Client.ReadIntFromInputStream("ChunkServerMaster", ois);
						String src1 = new String(Client.RecvPayload("ChunkServerMaster", ois, srcLen1));

						int destLen1 = Client.ReadIntFromInputStream("ChunkServerMaster", ois);
						String dest1 = new String(Client.RecvPayload("ChunkServerMaster", ois, destLen1));

						// resp format: <FSReturnVal.ordinal()>
						oos.writeInt(createDir(src1, dest1).ordinal());
						oos.flush();

						break;
					case ClientFS.LIST_DIR_COMMAND:
						// req format: <dirname>
						int destLen2 = Client.ReadIntFromInputStream("ChunkServerMaster", ois);
						String dest2 = new String(Client.RecvPayload("ChunkServerMaster", ois, destLen2));

						// resp format: <resultsLen - string-1-len - string-1 - string-2-len - string-2 -
						// ... - string-resultsLen-len - string-resultsLen>
						List<String> results = new ArrayList<String>();
						FSReturnVals code = listDir(dest2 + "/", results);

						if (code == FSReturnVals.Success) {
							oos.writeInt(results.size());

							for (String result : results) {
								byte[] resultBuf = result.getBytes();

								oos.writeInt(resultBuf.length);
								oos.write(resultBuf);
							}
						} else {
							// TODO CL: is this good enough off case handling
							oos.writeInt(-1);
						}
						oos.flush();

						break;
					case ClientFS.DELETE_DIR_COMMAND:
						// req format: <srcLen - srcBytes - destLen - destBytes>
						int srcLen3 = Client.ReadIntFromInputStream("ChunkServerMaster", ois);
						String src3 = new String(Client.RecvPayload("ChunkServerMaster", ois, srcLen3));

						int destLen3 = Client.ReadIntFromInputStream("ChunkServerMaster", ois);
						String dest3 = new String(Client.RecvPayload("ChunkServerMaster", ois, destLen3));

						// resp format: <FSReturnVal.ordinal()>
						oos.writeInt(deleteDir(src3, dest3).ordinal());
						oos.flush();

						break;
					case ClientFS.RENAME_DIR_COMMAND:
						// req format: <origLen - origBytes - newNameLen - newNameBytes>
						int srcLen4 = Client.ReadIntFromInputStream("ChunkServerMaster", ois);
						String src4 = new String(Client.RecvPayload("ChunkServerMaster", ois, srcLen4));

						int destLen4 = Client.ReadIntFromInputStream("ChunkServerMaster", ois);
						String dest4 = new String(Client.RecvPayload("ChunkServerMaster", ois, destLen4));
						
						// resp format: <FSReturnVal.ordinal()>
						oos.writeInt(renameDir(src4, dest4).ordinal());
						oos.flush();
						
						break;
					case ClientFS.CREATE_FILE_COMMAND:
						// req format: <parentLen - parentBytes - nameLen - nameBytes>
						int parentLen1 = Client.ReadIntFromInputStream("ChunkServerMaster", ois);
						String parent1 = new String(Client.RecvPayload("ChunkServerMaster", ois, parentLen1));

						int nameLen1 = Client.ReadIntFromInputStream("ChunkServerMaster", ois);
						String name1 = new String(Client.RecvPayload("ChunkServerMaster", ois, nameLen1));

						// resp format: <FSReturnVal.ordinal()>
						oos.writeInt(createFile(parent1, name1).ordinal());
						oos.flush();

						break;
					case ClientFS.DELETE_FILE_COMMAND:
						// req format: <parentLen - parentBytes - nameLen - nameBytes>
						int parentLen2 = Client.ReadIntFromInputStream("ChunkServerMaster", ois);
						String parent2 = new String(Client.RecvPayload("ChunkServerMaster", ois, parentLen2));

						int nameLen2 = Client.ReadIntFromInputStream("ChunkServerMaster", ois);
						String name2 = new String(Client.RecvPayload("ChunkServerMaster", ois, nameLen2));

						// resp format: <FSReturnVal.ordinal()>
						oos.writeInt(deleteFile(parent2, name2).ordinal());
						oos.flush();

						break;
						
					case ClientRec.GET_LAST_CHUNK_COMMAND:
						int filenameLen = Client.ReadIntFromInputStream("ChunkServerMaster", ois);
						String filename = new String(Client.RecvPayload("ChunkServerMaster", ois, filenameLen));
						byte [] chunkhandle = getLastChunk(filename).getBytes();
						oos.writeInt(chunkhandle.length);
						oos.write(chunkhandle);
						oos.flush();
						
					case ClientRec.ADD_CHUNK_COMMAND:
						int filename2Len = Client.ReadIntFromInputStream("ChunkServerMaster", ois);
						String filename2 = new String(Client.RecvPayload("ChunkServerMaster", ois, filename2Len));
						byte [] chunkhandle2 = addChunk(filename2).getBytes();
						oos.writeInt(chunkhandle2.length);
						oos.write(chunkhandle2);
						oos.flush();
						
					case ClientRec.GET_NUM_CHUNKS_COMMAND:
						int filename3Len = Client.ReadIntFromInputStream("ChunkServerMaster", ois);
						String filename3 = new String(Client.RecvPayload("ChunkServerMaster", ois, filename3Len));
						oos.writeInt(getNumChunks(filename3));
						oos.flush();
								
					case ClientRec.GET_CHUNK_COMMAND:
						int filename4Len = Client.ReadIntFromInputStream("ChunkServerMaster", ois);
						String filename4 = new String(Client.RecvPayload("ChunkServerMaster", ois, filename4Len));
						int chunkIndex = Client.ReadIntFromInputStream("ChunkServerMaster", ois);
						oos.write(getChunk(filename4, chunkIndex).getBytes());
						oos.flush();		
							
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
	private String getLastChunk (String filepath) {
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

	// SP: Added for appendRecord functionality
	private String addChunk(String filepath) {
		String newChunkhandle = "None";
		if (this.namespace.containsKey(filepath)) {
			newChunkhandle = String.valueOf(ChunkIndex);
			this.namespace.get(filepath).add(newChunkhandle);
			ChunkIndex +=1;
		}
		return newChunkhandle;
	}
	
	// SP: Added for getFirstRecord functionality
	private String getChunk (String filepath, int chunkIndex) {
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
	
	// SP: Added for getFirstRecord functionality
	private int getNumChunks (String filepath) {
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
