package com.chunkserver;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

public class ChunkServerThread implements Runnable {
	private ChunkServer master;
	private Socket channel;
	
	public ChunkServerThread(Socket socket, ChunkServer master) {
		this.master = master;
		this.channel = socket;
	}
	
	public void run() {
		try {
			InputStream in = this.channel.getInputStream();
			
			for (byte[] payload = this.read(in); payload != null; payload = this.read(in))
				handleRequest(payload);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private void handleRequest(byte[] request) {
		try {
			OutputStream out = this.channel.getOutputStream();

			byte[] commandBuf = new byte[4];
			System.arraycopy(request, 0, commandBuf, 0, 4);
			int command = ByteBuffer.wrap(commandBuf).getInt();
			
			switch (command) {
			// init chunk
			case 0:
				write(out, master.initializeChunk().getBytes());
				break;
			// put chunk
			case 1:
				byte[] offsetBuf1 = new byte[4];
				byte[] dataLenBuf1 = new byte[4];
				byte[] chunkHandleLenBuf1 = new byte[4];
				
				System.arraycopy(request, 4, offsetBuf1, 0, 4);
				int offset1 = ByteBuffer.wrap(offsetBuf1).getInt();
				
				System.arraycopy(request, 8, dataLenBuf1, 0, 4);
				int dataLen1 = ByteBuffer.wrap(dataLenBuf1).getInt();
				
				byte[] dataBuf1 = new byte[dataLen1];
				System.arraycopy(request, 12, dataBuf1, 0, dataLen1);
				
				System.arraycopy(request,  12 + dataLen1, chunkHandleLenBuf1, 0, 4);
				int chunkHandleLen1 = ByteBuffer.wrap(chunkHandleLenBuf1).getInt();
				
				byte[] chunkHandleBuf1 = new byte[chunkHandleLen1];
				System.arraycopy(request, 16 + dataLen1, chunkHandleBuf1, 0, chunkHandleLen1);
				
				// boolean bufs for responses
				byte[] falseBuf = ByteBuffer.allocate(4).putInt(0).array();
				byte[] trueBuf = ByteBuffer.allocate(4).putInt(1).array();

				write(out, this.master.putChunk(new String(chunkHandleBuf1), dataBuf1, offset1)
						? trueBuf
						: falseBuf);
				
				break;
			// get chunk
			case 2:
				byte[] offsetBuf2 = new byte[4];
				byte[] dataLenBuf2 = new byte[4];
				byte[] chunkHandleLenBuf2 = new byte[4];

				System.arraycopy(request, 4, offsetBuf2, 0, 4);
				System.arraycopy(request, 8, dataLenBuf2, 0, 4);
				System.arraycopy(request, 12, chunkHandleLenBuf2, 0, 4);
				
				int offset2 = ByteBuffer.wrap(offsetBuf2).getInt();
				int dataLen2 = ByteBuffer.wrap(dataLenBuf2).getInt();
				int chunkHandleLen2 = ByteBuffer.wrap(chunkHandleLenBuf2).getInt();
				
				byte[] chunkHandleBuf2 = new byte[chunkHandleLen2];
				System.arraycopy(request, 16, chunkHandleBuf2, 0, chunkHandleLen2);
				
				byte[] got = this.master.getChunk(new String(chunkHandleBuf2), offset2, dataLen2);
				write(out, got);
				
				break;
			default:
				System.out.println(command);
				break;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
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
