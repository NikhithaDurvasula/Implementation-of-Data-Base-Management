package bufmgr;

import global.Page;
import global.PageId;

public class FrameDesc extends Page {

	// Page frame attributes.
	public PageId pageNum;
	public int pinCount;
	public boolean dirtyBit;
	public boolean referenceBit;
	public boolean validBit;

	public FrameDesc() {
		pageNum = new PageId();
		dirtyBit = false;
		referenceBit = false;
		validBit = false;
		pinCount = 0;
	}

	public void incPinCount() {
		pinCount = pinCount+1;
	}

	public void decPinCount() {
		pinCount = pinCount-1;
	}

	// Setting dirty bit
	public void setDirtyBit(boolean dirty) {
		dirtyBit = dirty;
	}

	// Setting valid bit
	public void setValidBit(boolean bit) {
		validBit = bit;
	}

	// Setting Reference bit
	public void setRefBit(boolean reference) {
		referenceBit = reference;
	}
	// To Get dirty status
	public boolean dirtyBit() {
		return dirtyBit;
	}

	//to Get reference bit
	public boolean refBit() {
		return referenceBit;
	}

	// to Get the valid bit
	public boolean validBit() {
		return validBit;
	}

	// To Get the page no of the page in frame.
	public PageId pageId() {
		return pageNum;
	}

}
