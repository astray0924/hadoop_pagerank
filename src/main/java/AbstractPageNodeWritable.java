import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.Writable;

public abstract class AbstractPageNodeWritable implements Writable {
	private VIntWritable id;
	private VIntWritable outCount;

	public AbstractPageNodeWritable() {
		this.id = new VIntWritable(0);
		this.outCount = new VIntWritable(0);
	}

	public AbstractPageNodeWritable(VIntWritable id, VIntWritable outCount) {
		this.id = id;
		this.outCount = outCount;
	}

	public AbstractPageNodeWritable(Integer id, Integer outCount) {
		this.id = new VIntWritable(id);
		this.outCount = new VIntWritable(outCount);
	}

	public VIntWritable getId() {
		return id;
	}

	public void setId(VIntWritable id) {
		this.id = id;
	}

	public VIntWritable getOutCount() {
		return outCount;
	}

	public void setOutCount(VIntWritable outCount) {
		this.outCount = outCount;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		id.write(out);
		outCount.write(out);

	}

	@Override
	public void readFields(DataInput in) throws IOException {
		id.readFields(in);
		outCount.readFields(in);
	}

	@Override
	public int hashCode() {
		return id.hashCode() * 517 + outCount.hashCode();
	}

}
