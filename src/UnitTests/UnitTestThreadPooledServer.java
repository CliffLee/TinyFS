package UnitTests;

import java.nio.ByteBuffer;

import com.chunkserver.ChunkServer;
import com.client.Client;

/**
 * UnitTest1 for Part 1 of TinyFS
 * 
 * @author Shahram Ghandeharizadeh
 *
 */
public class UnitTestThreadPooledServer {

	/**
	 * This unit test generates an array of 4K bytes where each byte = "1"
	 */
	public static String handle = null;

	public static void main(String[] args) {
		test1();
	}

	public static void test1() {
		Client client = new Client();
		System.out.println(client.initializeChunk());
	}

}
