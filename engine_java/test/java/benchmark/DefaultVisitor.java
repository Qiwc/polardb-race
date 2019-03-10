package benchmark;

import java.util.concurrent.atomic.AtomicInteger;

import com.alibabacloud.polar_race.engine.common.AbstractVisitor;

public class DefaultVisitor extends AbstractVisitor {

	public AtomicInteger visitCount = new AtomicInteger();
	
	public void visit(byte[] key, byte[] value) {
		visitCount.incrementAndGet();
	}

}
