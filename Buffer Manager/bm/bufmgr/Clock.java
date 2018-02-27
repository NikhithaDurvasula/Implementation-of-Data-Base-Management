package bufmgr;

public class Clock {
	// Current pool of frames
	public FrameDesc[] currentPool;
	//current pointer in the pool
	public int currentVar = 0;

	public Clock(FrameDesc[] bufferPool) {
		currentPool = bufferPool;
	}

	// Clock implementation to choose the victim frame.
	public int pickVictim() {
		for (int counter = 0; counter < currentPool.length * 2; counter++) {
			if (!currentPool[currentVar].validBit) {
				return currentVar;
			}
			if (currentPool[currentVar].pinCount == 0) {
				// checkinf if there is a referenceBit in the current pool
				if (currentPool[currentVar].referenceBit == true) {
					currentPool[currentVar].setRefBit(false);
				} else {
					return currentVar;
				}
			}
            currentVar = (currentVar+1) % currentPool.length;
		}
		return -1;
	}
}
