package ac.kr.kaist.kyoungrok.hadoop_pagerank_new.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.VIntWritable;

public class PageRankNode extends AbstractPageNodeWritableComparable {
	private VIntArrayWritable inLinks = new VIntArrayWritable();

	public String toString() {
		return String
				.format("RANK: {ID: %s, INLINKS: %s, INCOUNT: %s, OUTCOUNT: %s, SCORE: %s}",
						id, StringUtils.join(inLinks.toStrings(), ","),
						inLinks.getSize(), outCount, score);
	}

	public PageRankNode() {
		super();
		this.inLinks = new VIntArrayWritable();
	}

	public PageRankNode(VIntWritable id, VIntWritable outCount,
			VIntArrayWritable inLinks, FloatWritable score) {
		super(id, outCount, score);
		this.inLinks = inLinks;
	}

	public PageRankNode(Integer id, Integer outCount, List<Integer> inLinks,
			Float score) {
		super(id, outCount, score);

		List<VIntWritable> temp = new ArrayList<VIntWritable>();
		for (Integer in : inLinks) {
			temp.add(new VIntWritable(in));
		}
		VIntWritable[] inLinksArray = temp
				.toArray(new VIntWritable[temp.size()]);
		this.inLinks.set(inLinksArray);
	}

	public static PageRankNode fromPageMetaNode(PageMetaNode metaNode) {
		return new PageRankNode(metaNode.getId(), metaNode.outCount,
				new VIntArrayWritable(), metaNode.getScore());
	}

	public void set(VIntWritable id, VIntWritable outCount,
			VIntArrayWritable inLinks, FloatWritable score) {
		super.set(id, outCount, score);
		this.inLinks = inLinks;
	}
	
	public void updateScore(FloatWritable score) {
		this.score = score;
	}
	
	public void updateScore(float score) {
		this.updateScore(new FloatWritable(score));
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
