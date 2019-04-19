package com.client;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import com.chunkserver.ChunkServer;
import com.chunkserver.ChunkServerMaster;

public class ClientFS {

	public enum FSReturnVals {
		DirExists, // Returned by CreateDir when directory exists
		DirNotEmpty, //Returned when a non-empty directory is deleted
		SrcDirNotExistent, // Returned when source directory does not exist
		DestDirExists, // Returned when a destination directory exists
		DestDirNotExistent, // CL: We should have this for delete dir no?
		FileExists, // Returned when a file exists
		FileDoesNotExist, // Returns when a file does not exist
		BadHandle, // Returned when the handle for an open file is not valid
		RecordTooLong, // Returned when a record size is larger than chunk size
		BadRecID, // The specified RID is not valid, used by DeleteRecord
		RecDoesNotExist, // The specified record does not exist, used by DeleteRecord
		NotImplemented, // Specific to CSCI 485 and its unit tests
		Success, //Returned when a method succeeds
		Fail //Returned when a method fails
	}
	
	// Directory CRUD codes
	public static final int CREATE_DIR_COMMAND = 0;
	public static final int LIST_DIR_COMMAND = 1;
	public static final int DELETE_DIR_COMMAND = 2;
	public static final int RENAME_DIR_COMMAND = 3;

	// File CRUD codes
	public static final int CREATE_FILE_COMMAND = 4;
	public static final int OPEN_FILE_COMMAND = 5;
	public static final int CLOSE_FILE_COMMAND = 6;
	
	static int ServerPort = 0;
	static Socket ClientSocket;
	static ObjectOutputStream WriteOutput;
	static ObjectInputStream ReadInput;
	
	public ClientFS() {
		if (ClientSocket != null) return; //The client is already connected
		try {
			BufferedReader binput = new BufferedReader(new FileReader(ChunkServerMaster.MasterConfigFile));
			String port = binput.readLine();
			port = port.substring( port.indexOf(':')+1 );
			ServerPort = Integer.parseInt(port);
			
			ClientSocket = new Socket("127.0.0.1", ServerPort);
			WriteOutput = new ObjectOutputStream(ClientSocket.getOutputStream());
			ReadInput = new ObjectInputStream(ClientSocket.getInputStream());
			
			System.out.printf("ClientFS connecting to port %d...\n", ServerPort);
		}catch (FileNotFoundException e) {
			System.out.println("Error (Client), the config file "+ ChunkServer.ClientConfigFile +" containing the port of the ChunkServer is missing.");
		}catch (IOException e) {
			System.out.println("Can't find file.");
		}
	}

	/**
	 * Creates the specified dirname in the src directory Returns
	 * SrcDirNotExistent if the src directory does not exist Returns
	 * DestDirExists if the specified dirname exists
	 *
	 * Example usage: CreateDir("/", "Shahram"), CreateDir("/Shahram/",
	 * "CSCI485"), CreateDir("/Shahram/CSCI485/", "Lecture1")
	 */
	public FSReturnVals CreateDir(String src, String dirname) {
		// determine src length
		byte[] bsrc = src.getBytes();
		int srcLen = bsrc.length;
		
		// determine dest length
		byte[] bdest = dirname.getBytes();
		int destLen = bdest.length;
		
		// write payload size
		int payloadSize = 4 + 4 + 4 + srcLen + 4 + destLen;
		
		try {
			WriteOutput.writeInt(payloadSize);
			WriteOutput.writeInt(CREATE_DIR_COMMAND);
			WriteOutput.writeInt(srcLen);
			WriteOutput.write(bsrc);
			WriteOutput.writeInt(destLen);
			WriteOutput.write(bdest);
			WriteOutput.flush();
			
			int response = Client.ReadIntFromInputStream("ClientFS", ReadInput);
			
			return FSReturnVals.values()[response];
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return FSReturnVals.Fail;
	}

	/**
	 * Deletes the specified dirname in the src directory Returns
	 * SrcDirNotExistent if the src directory does not exist Returns
	 * DestDirExists if the specified dirname exists
	 *
	 * Example usage: DeleteDir("/Shahram/CSCI485/", "Lecture1")
	 */
	public FSReturnVals DeleteDir(String src, String dirname) {
		// determine src length
		byte[] bsrc = src.getBytes();
		int srcLen = bsrc.length;
		
		// determine dest length
		byte[] bdest = dirname.getBytes();
		int destLen = bdest.length;
		
		// write payload size
		int payloadSize = 4 + 4 + 4 + srcLen + 4 + destLen;

		try {
			WriteOutput.writeInt(payloadSize);
			WriteOutput.writeInt(DELETE_DIR_COMMAND);
			WriteOutput.writeInt(srcLen);
			WriteOutput.write(bsrc);
			WriteOutput.writeInt(destLen);
			WriteOutput.write(bdest);
			WriteOutput.flush();

			int response = Client.ReadIntFromInputStream("ClientFS", ReadInput);

			return FSReturnVals.values()[response];
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return FSReturnVals.Fail;
	}

	/**
	 * Renames the specified src directory in the specified path to NewName
	 * Returns SrcDirNotExistent if the src directory does not exist Returns
	 * DestDirExists if a directory with NewName exists in the specified path
	 *
	 * Example usage: RenameDir("/Shahram/CSCI485", "/Shahram/CSCI550") changes
	 * "/Shahram/CSCI485" to "/Shahram/CSCI550"
	 */
	public FSReturnVals RenameDir(String src, String NewName) {
		byte[] bold = src.getBytes();
		int oldLen = bold.length;
		
		byte[] bnew = NewName.getBytes();
		int newLen = bnew.length;
		
		int payloadSize = 4 + 4 + 4 + oldLen + 4 + newLen;
		
		try {
			WriteOutput.writeInt(payloadSize);
			WriteOutput.writeInt(RENAME_DIR_COMMAND);
			WriteOutput.writeInt(oldLen);
			WriteOutput.write(bold);
			WriteOutput.writeInt(newLen);
			WriteOutput.write(bnew);
			WriteOutput.flush();
			
			int response = Client.ReadIntFromInputStream("ClientFS", ReadInput);
			
			return FSReturnVals.values()[response];
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return FSReturnVals.Fail;
	}

	/**
	 * Lists the content of the target directory Returns SrcDirNotExistent if
	 * the target directory does not exist Returns null if the target directory
	 * is empty
	 *
	 * Example usage: ListDir("/Shahram/CSCI485")
	 */
	public String[] ListDir(String tgt) {
		List<String> results = new ArrayList<String>();

		try {
			byte[] tgtBuf = tgt.getBytes();

			WriteOutput.writeInt(12 + tgtBuf.length);
			WriteOutput.writeInt(LIST_DIR_COMMAND);
			WriteOutput.writeInt(tgtBuf.length);
			WriteOutput.write(tgtBuf);
			WriteOutput.flush();

			int resultsLen = Client.ReadIntFromInputStream("ClientFS", ReadInput);
			if (resultsLen == -1) {
				return null;
			} else {
				for (int i=0; i<resultsLen; i++) {
					int lsLen = Client.ReadIntFromInputStream("ClientFS", ReadInput);
					String lsName = new String(Client.RecvPayload("ClientFS", ReadInput, lsLen));

					results.add(lsName);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		return results.isEmpty()
			? null
			: results.toArray(new String[results.size()]);
	}

	/**
	 * Creates the specified filename in the target directory Returns
	 * SrcDirNotExistent if the target directory does not exist Returns
	 * FileExists if the specified filename exists in the specified directory
	 *
	 * Example usage: Createfile("/Shahram/CSCI485/Lecture1/", "Intro.pptx")
	 */
	public FSReturnVals CreateFile(String tgtdir, String filename) {
		byte[] parent = tgtdir.getBytes();
		int parentLen = parent.length;
		
		byte[] name = filename.getBytes();
		int nameLen = name.length;
		
		int payloadSize = 4 + 4 + 4 + parentLen + 4 + nameLen;
		
		try {
			WriteOutput.writeInt(payloadSize);
			WriteOutput.writeInt(CREATE_FILE_COMMAND);
			WriteOutput.writeInt(parentLen);
			WriteOutput.write(parent);
			WriteOutput.writeInt(nameLen);
			WriteOutput.write(name);
			WriteOutput.flush();
			
			int response = Client.ReadIntFromInputStream("ClientFS", ReadInput);
			
			return FSReturnVals.values()[response];
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return FSReturnVals.Fail;
	}

	/**
	 * Deletes the specified filename from the tgtdir Returns SrcDirNotExistent
	 * if the target directory does not exist Returns FileDoesNotExist if the
	 * specified filename is not-existent
	 *
	 * Example usage: DeleteFile("/Shahram/CSCI485/Lecture1/", "Intro.pptx")
	 */
	public FSReturnVals DeleteFile(String tgtdir, String filename) {
		byte[] parent = tgtdir.getBytes();
		int parentLen = parent.length;
		
		byte[] name = filename.getBytes();
		int nameLen = name.length;
		
		int payloadSize = 4 + 4 + 4 + parentLen + 4 + nameLen;
		
		try {
			WriteOutput.writeInt(payloadSize);
			WriteOutput.writeInt(DELETE_FILE_COMMAND);
			WriteOutput.writeInt(parentLen);
			WriteOutput.write(parent);
			WriteOutput.writeInt(nameLen);
			WriteOutput.write(name);
			WriteOutput.flush();
			
			int response = Client.ReadIntFromInputStream("ClientFS", ReadInput);
			
			return FSReturnVals.values()[response];
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return FSReturnVals.Fail;
	}

	/**
	 * Opens the file specified by the FilePath and populates the FileHandle
	 * Returns FileDoesNotExist if the specified filename by FilePath is
	 * not-existent
	 *
	 * Example usage: OpenFile("/Shahram/CSCI485/Lecture1/Intro.pptx", FH1)
	 */
	public FSReturnVals OpenFile(String FilePath, FileHandle ofh) {
		// TODO CL: temp
		ofh.filepath = FilePath;
		return FSReturnVals.Success;
	}

	/**
	 * Closes the specified file handle Returns BadHandle if ofh is invalid
	 *
	 * Example usage: CloseFile(FH1)
	 */
	public FSReturnVals CloseFile(FileHandle ofh) {
		return null;
	}

}
