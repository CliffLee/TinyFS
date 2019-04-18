package com.client;

public class RID {
	// ClientRec ReadFirstRecord
	public String chunkHandle;
	public int slotNumber;
	
	public RID() {
		chunkHandle = "somethingstupid";
		slotNumber = 0;
	}

	public String resolve() {
		return chunkHandle + String.valueOf(slotNumber);
	}
}
