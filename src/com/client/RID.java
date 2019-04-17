package com.client;

public class RID {
	// ClientRec ReadFirstRecord
	public String chunkHandle;
	public int slotNumber;
	
	public RID() {
		macAddress = "somethingstupid";
		key = 0;
	}

	public String resolve() {
		return macAddress + String.valueOf(this.key);
	}
}
