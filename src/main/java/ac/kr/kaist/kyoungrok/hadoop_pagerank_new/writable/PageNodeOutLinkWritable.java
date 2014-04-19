package ac.kr.kaist.kyoungrok.hadoop_pagerank_new.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;

public class PageNodeOutLinkWritable extends AbstractPageNodeWritable {
	private TextArrayWritable outLinks;

	public PageNodeOutLinkWritable() {
		super();
		this.outLinks = new TextArrayWritable();
	}

	public PageNodeOutLinkWritable(Integer id, List<String> outLinks,
			Integer outCount, Float score) {
		super(id, outCount, score);
		Text[] outLinksArray = outLinks.toArray(new Text[] { new Text("") });
		this.outLinks.set(outLinksArray);
	}

	public PageNodeOutLinkWritable(VIntWritable id, VIntWritable outCount,
			TextArrayWritable outLinks, FloatWritable score) {
		super(id, outCount, score);
		this.outLinks = outLinks;
	}

	public TextArrayWritable getOutLinks() {
		return outLinks;
	}

	public void setOutLinks(TextArrayWritable outLinks) {
		this.outLinks = outLinks;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		outLinks.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		outLinks.readFields(in);
	}

	@Override
	public int hashCode() {
		return super.hashCode() + getOutLinks().hashCode();
	}

}
