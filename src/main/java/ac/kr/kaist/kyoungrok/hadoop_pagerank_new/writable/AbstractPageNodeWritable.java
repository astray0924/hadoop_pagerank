package ac.kr.kaist.kyoungrok.hadoop_pagerank_new.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.Writable;

public abstract class AbstractPageNodeWritable implements Writable {
	private VIntWritable id;
	private VIntWritable outCount;
	private FloatWritable score;

	public AbstractPageNodeWritable() {
		this.id = new VIntWritable();
		this.outCount = new VIntWritable();
		this.score = new FloatWritable();
	}

	public AbstractPageNodeWritable(VIntWritable id, VIntWritable outCount,
			FloatWritable score) {
		this.id = id;
		this.outCount = outCount;
		this.score = score;
	}

	public AbstractPageNodeWritable(Integer id, Integer outCount, Float score) {
		this.id = new VIntWritable(id);
		this.outCount = new VIntWritable(outCount);
		this.score = new FloatWritable(score);
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

	public FloatWritable getScore() {
		return score;
	}

	public void setScore(FloatWritable score) {
		this.score = score;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		id.write(out);
		outCount.write(out);
		score.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		id.readFields(in);
		outCount.readFields(in);
		score.readFields(in);
	}

	@Override
	public int hashCode() {
		return id.hashCode() * 517 + outCount.hashCode() + 227
				* score.hashCode();
	}

}
