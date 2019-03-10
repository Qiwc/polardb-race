package benchmark;

import java.util.concurrent.Callable;

public class MemChecker implements Callable<Long>, Runnable{
	public long currentMemory = 0;
	
	public MemChecker(long currentMemory) {
		this.currentMemory = currentMemory;
	}

	public Long call() throws Exception {
		if(Runtime.getRuntime().totalMemory() > currentMemory)
			currentMemory = Runtime.getRuntime().totalMemory();
		return currentMemory;
	}

	public void run() {
		if(Runtime.getRuntime().totalMemory() > this.currentMemory)
			this.currentMemory = Runtime.getRuntime().totalMemory();
	}
	
	public Long getCurrentMemoryMb() {
		return currentMemory / (1024 * 1024);
	}
}
