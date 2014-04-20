package ac.kr.kaist.kyoungrok.hadoop_pagerank_new.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.VIntWritable;

public class PageRankNodeWritable extends AbstractPageNodeWritableComparable {
	private VIntArrayWritable inLinks = new VIntArrayWritable();

	public PageRankNodeWritable() {
		super();
		this.inLinks = new VIntArrayWritable();
	}

	public PageRankNodeWritable(VIntWritable id, VIntWritable outCount,
			VIntArrayWritable inLinks, FloatWritable score) {
		super(id, outCount, score);
		this.inLinks = inLinks;
	}

	public PageRankNodeWritable(Integer id, Integer outCount,
			List<Integer> inLinks, Float score) {
		super(id, outCount, score);
		VIntWritable[] inLinksArray = inLinks
				.toArray(new VIntWritable[] { new VIntWritable(0) });
		this.inLinks.set(inLinksArray);
	}

	public PageRankNodeWritable(PageMetaNodeWritable metaNode) {
		this(metaNode.getId(), metaNode.outCount, new VIntArrayWritable(),
				metaNode.getScore());
	}

	public VIntArrayWritable getInLinks() {
		return inLinks;
	}

	public void setInLinks(VIntArrayWritable inLinks) {
		this.inLinks = inLinks;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		inLinks.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		inLinks.readFields(in);
	}

	@Override
	public int hashCode() {
		return super.hashCode() + 123 * inLinks.hashCode();
	}

}
