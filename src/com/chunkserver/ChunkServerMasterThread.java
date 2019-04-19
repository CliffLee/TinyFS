package com.chunkserver;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import com.client.Client;
import com.client.ClientFS;
import com.client.ClientFS.FSReturnVals;
import com.client.ClientRec;

public class ChunkServerMasterThread implements Runnable
{
	private Socket connection;
	private ChunkServerMaster master;

	private ObjectInputStream ois;
	private ObjectOutputStream oos;

	public ChunkServerMasterThread(Socket socket, ChunkServerMaster master) {
		this.connection = socket;
		this.master = master;
	}

	public void run() {
		try {
			ois = new ObjectInputStream(connection.getInputStream());
			oos = new ObjectOutputStream(connection.getOutputStream());
			System.out.println("Started a new connection");

			while (!connection.isClosed()) {
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
						oos.writeInt(master.createDir(src1, dest1).ordinal());
						oos.flush();

						break;
					case ClientFS.LIST_DIR_COMMAND:
						// req format: <dirname>
						int destLen2 = Client.ReadIntFromInputStream("ChunkServerMaster", ois);
						String dest2 = new String(Client.RecvPayload("ChunkServerMaster", ois, destLen2));

						// resp format: <resultsLen - string-1-len - string-1 - string-2-len - string-2 -
						// ... - string-resultsLen-len - string-resultsLen>
						List<String> results = new ArrayList<String>();
						FSReturnVals code = master.listDir(dest2 + "/", results);

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
						oos.writeInt(master.deleteDir(src3, dest3).ordinal());
						oos.flush();

						break;
					case ClientFS.RENAME_DIR_COMMAND:
						// req format: <origLen - origBytes - newNameLen - newNameBytes>
						int srcLen4 = Client.ReadIntFromInputStream("ChunkServerMaster", ois);
						String src4 = new String(Client.RecvPayload("ChunkServerMaster", ois, srcLen4));

						int destLen4 = Client.ReadIntFromInputStream("ChunkServerMaster", ois);
						String dest4 = new String(Client.RecvPayload("ChunkServerMaster", ois, destLen4));

						// resp format: <FSReturnVal.ordinal()>
						oos.writeInt(master.renameDir(src4, dest4).ordinal());
						oos.flush();

						break;
					case ClientFS.CREATE_FILE_COMMAND:
						// req format: <parentLen - parentBytes - nameLen - nameBytes>
						int parentLen1 = Client.ReadIntFromInputStream("ChunkServerMaster", ois);
						String parent1 = new String(Client.RecvPayload("ChunkServerMaster", ois, parentLen1));

						int nameLen1 = Client.ReadIntFromInputStream("ChunkServerMaster", ois);
						String name1 = new String(Client.RecvPayload("ChunkServerMaster", ois, nameLen1));

						// resp format: <FSReturnVal.ordinal()>
						oos.writeInt(master.createFile(parent1, name1).ordinal());
						oos.flush();

						break;
					case ClientFS.DELETE_FILE_COMMAND:
						// req format: <parentLen - parentBytes - nameLen - nameBytes>
						int parentLen2 = Client.ReadIntFromInputStream("ChunkServerMaster", ois);
						String parent2 = new String(Client.RecvPayload("ChunkServerMaster", ois, parentLen2));

						int nameLen2 = Client.ReadIntFromInputStream("ChunkServerMaster", ois);
						String name2 = new String(Client.RecvPayload("ChunkServerMaster", ois, nameLen2));

						// resp format: <FSReturnVal.ordinal()>
						oos.writeInt(master.deleteFile(parent2, name2).ordinal());
						oos.flush();

						break;
						
					case ClientRec.GET_LAST_CHUNK_COMMAND:
						int filenameLen = Client.ReadIntFromInputStream("ChunkServerMaster", ois);
						String filename = new String(Client.RecvPayload("ChunkServerMaster", ois, filenameLen));
						byte [] chunkhandle = master.getLastChunk(filename).getBytes();
						oos.writeInt(chunkhandle.length);
						oos.write(chunkhandle);
						oos.flush();
						break;
						
					case ClientRec.ADD_CHUNK_COMMAND:
						int filename2Len = Client.ReadIntFromInputStream("ChunkServerMaster", ois);
						String filename2 = new String(Client.RecvPayload("ChunkServerMaster", ois, filename2Len));
						byte [] chunkhandle2 = master.addChunk(filename2).getBytes();
						oos.writeInt(chunkhandle2.length);
						oos.write(chunkhandle2);
						oos.flush();
						break;
						
					case ClientRec.GET_NUM_CHUNKS_COMMAND:
						int filename3Len = Client.ReadIntFromInputStream("ChunkServerMaster", ois);
						String filename3 = new String(Client.RecvPayload("ChunkServerMaster", ois, filename3Len));
						oos.writeInt(master.getNumChunks(filename3));
						oos.flush();
						break;
								
					case ClientRec.GET_CHUNK_COMMAND:
						int filename4Len = Client.ReadIntFromInputStream("ChunkServerMaster", ois);
						String filename4 = new String(Client.RecvPayload("ChunkServerMaster", ois, filename4Len));
						int chunkIndex = Client.ReadIntFromInputStream("ChunkServerMaster", ois);
						byte [] chunkhandleName = master.getChunk(filename4, chunkIndex).getBytes();
						oos.writeInt(chunkhandleName.length);
						oos.write(chunkhandleName);
						oos.flush();	
						break;
						
					default:
						break;
				}
			}
		} catch(IOException e){
			e.printStackTrace();
		} finally {
			try {
				if (oos != null)
					oos.close();
				if (ois!= null)
					ois.close();
				if (connection != null)
					connection.close();
			} catch (IOException e) {
				System.out.println("ERR: Failed to close client socket/resources!");
			}
		} 

	}
}
