package ac.kr.kaist.kyoungrok.hadoop_pagerank.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.WritableComparable;

public class PageRankResult implements WritableComparable {
	private Text title;
	private VIntWritable id;
	private FloatWritable score;
	
	@Override
	public String toString() {
		return String.format("%s %s", title, score);
	}

	public PageRankResult() {
		this.title = new Text();
		this.id = new VIntWritable(0);
		this.score = new FloatWritable(0.0f);
	}

	public void set(Text title, VIntWritable id, FloatWritable score) {
		this.title = title;
		this.id = id;
		this.score = score;
	}

	public Text getTitle() {
		return title;
	}

	public void setTitle(Text title) {
		this.title = title;
	}

	public VIntWritable getId() {
		return id;
	}

	public void setId(VIntWritable id) {
		this.id = id;
	}

	public FloatWritable getScore() {
		return score;
	}

	public void setScore(FloatWritable score) {
		this.score = score;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		title.write(out);
		id.write(out);
		score.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		title.readFields(in);
		id.readFields(in);
		score.readFields(in);
	}

	@Override
	public int compareTo(Object obj) {
		PageRankResult compareTo = (PageRankResult) obj;
		return -score.compareTo(compareTo.getScore());
	}

}
