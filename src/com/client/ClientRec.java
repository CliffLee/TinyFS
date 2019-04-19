package com.client;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;

import com.client.ClientFS.FSReturnVals;
import com.chunkserver.ChunkServer;
import com.chunkserver.ChunkServerMaster;
import com.client.RID;

public class ClientRec {

	// Directory CRUD codes
	public static final int CREATE_DIR_COMMAND = 0;
	public static final int LIST_DIR_COMMAND = 1;
	public static final int DELETE_DIR_COMMAND = 2;
	public static final int RENAME_DIR_COMMAND = 3;

	// File CRUD codes
	public static final int CREATE_FILE_COMMAND = 4;
	public static final int OPEN_FILE_COMMAND = 5;
	public static final int DELETE_FILE_COMMAND = 6;
	
	
	// SP: Added for ClientRec
	public static final int GET_LAST_CHUNK_COMMAND = 7;
	public static final int ADD_CHUNK_COMMAND = 8;
	public static final int GET_NUM_CHUNKS_COMMAND = 9;
	public static final int GET_CHUNK_COMMAND = 10;
	public static final int GET_CHUNK_INDEX_COMMAND = 11;
	
	public static final int RECORD_HEADER_SIZE = 4;
	public static final int RECORD_SLOT_SIZE = 4;
	
	
	
	static int MasterPort = 0;
	static Socket ClientMasterSocket;
	static ObjectOutputStream MasterWriteOutput;
	static ObjectInputStream MasterReadInput;
	
	// SP: ClientRec
	public ClientRec() {
		if (ClientMasterSocket != null) return; //The client is already connected
		BufferedReader binput = null;
		try {
			binput = new BufferedReader(new FileReader(ChunkServerMaster.MasterConfigFile));
			String port = binput.readLine();
			port = port.substring( port.indexOf(':')+1 );
			MasterPort = Integer.parseInt(port);
			
			ClientMasterSocket = new Socket("127.0.0.1", MasterPort);
			MasterWriteOutput = new ObjectOutputStream(ClientMasterSocket.getOutputStream());
			MasterReadInput = new ObjectInputStream(ClientMasterSocket.getInputStream());
			
			System.out.printf("ClientRec connecting to Master on port %d...\n", MasterPort);
			binput.close();
			
			} catch (FileNotFoundException e) {
				System.out.println("Error (Client), the config file "+ ChunkServer.ClientConfigFile +" containing the port of the ChunkServer is missing.");
				try {
					binput.close();
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			} catch (IOException e) {
				System.out.println("Can't find file.");
				try {
					binput.close();
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				} 
			}
	}
	
	/**
	 * Appends a record to the open file as specified by ofh Returns BadHandle
	 * if ofh is invalid Returns BadRecID if the specified RID is not null
	 * Returns RecordTooLong if the size of payload exceeds chunksize RID is
	 * null if AppendRecord fails
	 *
	 * Example usage: AppendRecord(FH1, obama, RecID1)
	 */
	public FSReturnVals AppendRecord(FileHandle ofh, byte[] payload, RID RecordID) {

		//Validity: check if the payload size is too large (greater than chunk size)
		if (payload.length > ChunkServer.ChunkSize) {
			return ClientFS.FSReturnVals.RecordTooLong;
		}
		//Get the last chunk in the file from master/or create a chunk if the file has no chunks
		try {
			byte [] filepath  = ofh.filepath.getBytes();
			int messageSize = 4 + 4 + 4 + filepath.length;
			MasterWriteOutput.writeInt(messageSize);
			MasterWriteOutput.writeInt(GET_LAST_CHUNK_COMMAND);
			MasterWriteOutput.writeInt(filepath.length);
			MasterWriteOutput.write(filepath);
			MasterWriteOutput.flush();
			int size = Client.ReadIntFromInputStream("Client Rec", MasterReadInput);
			byte [] responseBytes = Client.RecvPayload("Client Rec", MasterReadInput, size);
			Client c = new Client ();
			String chunkhandle;
			String response = new String (responseBytes);
			if (response.equals("None")) {
				//get create new chunk, pass in chunkhandle from master
				String newChunk = addChunk(filepath);
				if (newChunk.equals("None")) {
					return ClientFS.FSReturnVals.FileDoesNotExist;
				}
				chunkhandle = c.createChunk(newChunk);
			}
			else {
				chunkhandle = response;
			}

			byte [] bytes = new byte [4];
			//get number of records in chunk, using bytes #0-4 of the chunk
			bytes = c.readChunk(chunkhandle, 0, 4);
			int numRecords = ByteBuffer.wrap(bytes).getInt();
			
			//get offset of next available record space, using bytes #4-8 of the chunk
			bytes = c.readChunk(chunkhandle, 4, 4);
			int offset = ByteBuffer.wrap(bytes).getInt();
			
			//see if the chunk has enough space for the record else create a new chunk
			if (offset + RECORD_HEADER_SIZE + payload.length + RECORD_SLOT_SIZE > ChunkServer.ChunkSize - numRecords * 4) {
				String newChunk = addChunk(filepath);
				chunkhandle = c.createChunk(newChunk);
				offset = 8;
				numRecords = 0;
			}
			//add the offset+4 (start address) to the slot map of the chunk 
			c.writeChunk(chunkhandle, ByteBuffer.wrap(bytes).putInt(offset + 4).array(), ChunkServer.ChunkSize-(numRecords * 4)-4);
			//update the number of records (1st 4 bytes of chunk)
			c.writeChunk(chunkhandle, ByteBuffer.wrap(bytes).putInt(numRecords+1).array(), 0);
			//put the record size in the 1st 4 bytes at offset
			c.writeChunk(chunkhandle, ByteBuffer.wrap(bytes).putInt(payload.length).array(), offset);
			//put in the record data in the next n bytes
			c.writeChunk(chunkhandle, payload, offset+ RECORD_HEADER_SIZE);
			//update the new next available record
			c.writeChunk(chunkhandle, ByteBuffer.wrap(bytes).putInt(RECORD_HEADER_SIZE + payload.length + offset).array(), 4);
			//populate the record ID
			RecordID.slotNumber = numRecords+1;
			RecordID.chunkHandle = chunkhandle;
			//return success if you get to this step!
			return ClientFS.FSReturnVals.Success;		
	
		} catch (IOException e) {
			e.printStackTrace();
		}
		return ClientFS.FSReturnVals.Fail;
	}

	/**
	 * Deletes the specified record by RecordID from the open file specified by
	 * ofh Returns BadHandle if ofh is invalid Returns BadRecID if the specified
	 * RID is not valid Returns RecDoesNotExist if the record specified by
	 * RecordID does not exist.
	 *
	 * Example usage: DeleteRecord(FH1, RecID1)
	 */
	public FSReturnVals DeleteRecord(FileHandle ofh, RID RecordID) {
		
		if (RecordID == null)
		{
			return ClientFS.FSReturnVals.BadRecID;
		}
		int slot = RecordID.slotNumber;
		String chunkhandle = RecordID.chunkHandle;
		//TODO: make sure the master has the filehandle & corresponding chunk in its namespace or return bad RID
		Client c = new Client();
		byte [] bytes = new byte [4];
		//get number of records in chunk, using bytes #0-4 of the chunk
		bytes = c.readChunk(chunkhandle, 0, 4);
		int numRecords = ByteBuffer.wrap(bytes).getInt();
		if (numRecords < slot)
		{
			return ClientFS.FSReturnVals.RecDoesNotExist;
		}
		//null out the slot corresponding to the record
		c.writeChunk(chunkhandle, ByteBuffer.wrap(bytes).putInt(-1).array(), ChunkServer.ChunkSize-(4 * slot));

		return ClientFS.FSReturnVals.Success;
	}

	/**
	 * Reads the first record of the file specified by ofh into payload Returns
	 * BadHandle if ofh is invalid Returns RecDoesNotExist if the file is empty
	 *
	 * Example usage: ReadFirstRecord(FH1, tinyRec)
	 */
	public FSReturnVals ReadFirstRecord(FileHandle ofh, TinyRec rec) {
		try 
		 {
			byte [] filepath  = ofh.filepath.getBytes();
			MasterWriteOutput.writeInt(4 + 4 + 4 + filepath.length);
			MasterWriteOutput.writeInt(GET_NUM_CHUNKS_COMMAND);
			MasterWriteOutput.writeInt(filepath.length);
			MasterWriteOutput.write(filepath);
			MasterWriteOutput.flush();
			int numChunks = Client.ReadIntFromInputStream("Client Rec", MasterReadInput);
			if (numChunks == -1)
			{
				return ClientFS.FSReturnVals.BadHandle;   
			}
			String chunkhandle = null;
			int slot = -1;
			int chunkIndex = 0;
			Client c = new Client ();
			byte [] bytes = new byte [4];
			while (chunkIndex < numChunks)
			{
				MasterWriteOutput.writeInt(4 + 4 + 4 + filepath.length + 4);
				MasterWriteOutput.writeInt(GET_CHUNK_COMMAND);
				MasterWriteOutput.writeInt(filepath.length);
				MasterWriteOutput.write(filepath);
				MasterWriteOutput.writeInt(chunkIndex);				
				MasterWriteOutput.flush();
			
				int chunkhandleSize = Client.ReadIntFromInputStream("Client Rec", MasterReadInput);
				byte [] responseBytes = Client.RecvPayload("Client Rec", MasterReadInput, chunkhandleSize);
				if (responseBytes == null)
				{
					break;
				}
				chunkhandle = new String (responseBytes);
				//get number of records in chunk, using bytes #0-4 of the chunk
				bytes = c.readChunk(chunkhandle, 0, 4);
				int numRecords = ByteBuffer.wrap(bytes).getInt();
				for (int i = 0; i<numRecords; i++)
				{
					//see if the record's slot is valid
					byte [] recordOffsetBytes = c.readChunk(chunkhandle, ChunkServer.ChunkSize - RECORD_SLOT_SIZE * i - 4, 4);
					int recordOffset = ByteBuffer.wrap(recordOffsetBytes).getInt();
					if (recordOffset != -1)
					{
						rec.setRID(new RID(chunkhandle, i+1));
						bytes = c.readChunk(chunkhandle, recordOffset-4, 4);
						int recordSize = ByteBuffer.wrap(bytes).getInt();
						bytes = c.readChunk(chunkhandle, recordOffset, recordSize);
						rec.setPayload(bytes);						
						return ClientFS.FSReturnVals.Success;		
					}
				}
				chunkIndex++;
			}
			return ClientFS.FSReturnVals.RecDoesNotExist;
		 }
		
		catch (Exception e)
		{
			return ClientFS.FSReturnVals.Fail;
		}
	}

	/**
	 * Reads the last record of the file specified by ofh into payload Returns
	 * BadHandle if ofh is invalid Returns RecDoesNotExist if the file is empty
	 *
	 * Example usage: ReadLastRecord(FH1, tinyRec)
	 */
	public FSReturnVals ReadLastRecord(FileHandle ofh, TinyRec rec){
		return null;
	}

	/**
	 * Reads the next record after the specified pivot of the file specified by
	 * ofh into payload Returns BadHandle if ofh is invalid Returns
	 * RecDoesNotExist if the file is empty or pivot is invalid
	 *
	 * Example usage: 1. ReadFirstRecord(FH1, tinyRec1) 2. ReadNextRecord(FH1,
	 * rec1, tinyRec2) 3. ReadNextRecord(FH1, rec2, tinyRec3)
	 */
	public FSReturnVals ReadNextRecord(FileHandle ofh, RID pivot, TinyRec rec){
		try 
		 {
			byte [] filepath  = ofh.filepath.getBytes();
			MasterWriteOutput.writeInt(4 + 4 + 4 + filepath.length);
			MasterWriteOutput.writeInt(GET_NUM_CHUNKS_COMMAND);
			MasterWriteOutput.writeInt(filepath.length);
			MasterWriteOutput.write(filepath);
			MasterWriteOutput.flush();
			int numChunks = Client.ReadIntFromInputStream("Client Rec", MasterReadInput);
			if (numChunks == -1)
			{
				return ClientFS.FSReturnVals.BadHandle;   
			}
			
			String chunkhandle = pivot.chunkHandle;
			int slot = pivot.slotNumber;
			
			MasterWriteOutput.writeInt(4 + 4 + 4 + filepath.length + 4 + chunkhandle.getBytes().length);
			MasterWriteOutput.writeInt(GET_CHUNK_INDEX_COMMAND);
			MasterWriteOutput.writeInt(filepath.length);
			MasterWriteOutput.write(filepath);
			MasterWriteOutput.writeInt(chunkhandle.getBytes().length);
			MasterWriteOutput.write(chunkhandle.getBytes());			
			MasterWriteOutput.flush();
			int chunkIndex = Client.ReadIntFromInputStream("Client Rec", MasterReadInput);
			if (chunkIndex == -1)
			{
				return ClientFS.FSReturnVals.BadRecID;
			}
			Client c = new Client ();
			byte [] bytes = new byte [4];
			while (chunkIndex < numChunks)
			{
				MasterWriteOutput.writeInt(4 + 4 + 4 + filepath.length + 4);
				MasterWriteOutput.writeInt(GET_CHUNK_COMMAND);
				MasterWriteOutput.writeInt(filepath.length);
				MasterWriteOutput.write(filepath);
				MasterWriteOutput.writeInt(chunkIndex);				
				MasterWriteOutput.flush();
				
				int chunkhandleSize = Client.ReadIntFromInputStream("Client Rec", MasterReadInput);
				byte [] responseBytes = Client.RecvPayload("Client Rec", MasterReadInput, chunkhandleSize);
				if (responseBytes == null)
				{
					break;
				}
				chunkhandle = new String (responseBytes);
				//get number of records in chunk, using bytes #0-4 of the chunk
				bytes = c.readChunk(chunkhandle, 0, 4);
				int numRecords = ByteBuffer.wrap(bytes).getInt();
				for (int i = slot; i<numRecords; i++)
				{
					//see if the record's slot is valid
					byte [] recordOffsetBytes = c.readChunk(chunkhandle, ChunkServer.ChunkSize - RECORD_SLOT_SIZE * i - 4, 4);
					int recordOffset = ByteBuffer.wrap(recordOffsetBytes).getInt();
					if (recordOffset != -1)
					{
						rec.setRID(new RID(chunkhandle, i+1));
						bytes = c.readChunk(chunkhandle, recordOffset-4, 4);
						int recordSize = ByteBuffer.wrap(bytes).getInt();
						bytes = c.readChunk(chunkhandle, recordOffset, recordSize);
						rec.setPayload(bytes);	
						
						return ClientFS.FSReturnVals.Success;		
					}
				}
				slot = 0;
				chunkIndex++;
			}
			return ClientFS.FSReturnVals.RecDoesNotExist;
		 }
		
		catch (Exception e)
		{
			return ClientFS.FSReturnVals.Fail;
		}
	}

	/**
	 * Reads the previous record after the specified pivot of the file specified
	 * by ofh into payload Returns BadHandle if ofh is invalid Returns
	 * RecDoesNotExist if the file is empty or pivot is invalid
	 *
	 * Example usage: 1. ReadLastRecord(FH1, tinyRec1) 2. ReadPrevRecord(FH1,
	 * recn-1, tinyRec2) 3. ReadPrevRecord(FH1, recn-2, tinyRec3)
	 */
	public FSReturnVals ReadPrevRecord(FileHandle ofh, RID pivot, TinyRec rec){
		return null;		
	}
	
	// SP: Utility Methods
	String addChunk (byte [] filepath)
	{
		try 
		{
			MasterWriteOutput.writeInt(4 + 4 + 4 + filepath.length);
			MasterWriteOutput.writeInt(ADD_CHUNK_COMMAND);
			MasterWriteOutput.writeInt(filepath.length);
			MasterWriteOutput.write(filepath);
			MasterWriteOutput.flush();
			int size = Client.ReadIntFromInputStream("Client Rec", MasterReadInput);
			byte [] responseBytes = Client.RecvPayload("Client Rec", MasterReadInput, size);	
			String response = new String (responseBytes);
			return response;
		}
		catch (Exception e)
		{
			System.out.println("Error when trying to add a new chunk.");
		}
		return null;
	}
}


