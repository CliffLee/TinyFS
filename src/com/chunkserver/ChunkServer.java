package com.chunkserver;

import com.interfaces.ChunkServerInterface;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * implementation of interfaces at the chunkserver side
 * 
 * @author Clifford Lee 9477802367
 *
 */

public class ChunkServer implements ChunkServerInterface {
	final static String filePath = "C:\\Users\\shahram\\Documents\\TinyFS-2\\csci485Disk\\"; // or C:\\newfile.txt
	// final static String filePath = "C:\\Users\\leecliff\\Documents\\TinyFS-2\\csci485Disk\\"; // or C:\\newfile.txt
	public static long counter;

	/**
	 * Initialize the chunk server
	 */
	public ChunkServer() {
		System.out.println("Initializing chunk server...");
		
		// initialize static counter to number of files in ChunkServer dir
		ChunkServer.counter = new File(filePath).list().length;
	}

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
