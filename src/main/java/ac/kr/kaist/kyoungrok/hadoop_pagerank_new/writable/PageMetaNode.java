package ac.kr.kaist.kyoungrok.hadoop_pagerank_new.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;

public class PageMetaNode extends AbstractPageNodeWritableComparable {
	private Text title = new Text();
	private TextArrayWritable outLinks = new TextArrayWritable();

	@Override
	public String toString() {
		return String
				.format("META: {ID: %s, \tTITLE: %s, \tOUTLINKS: %s, \tOUTCOUNT: %s, \tSCORE: %s}",
						id, title, StringUtils.join(outLinks.toStrings(), ","),
						outCount, score);
	}

	public PageMetaNode() {
		super();
		this.title = new Text("");
		this.outLinks = new TextArrayWritable();
	}

	public PageMetaNode(Integer id, String title, List<String> outLinks,
			Integer outCount, Float score) {
		super(id, outCount, score);

		// title
		this.title = new Text(title);

		// Out Links
		List<Text> temp = new ArrayList<Text>();
		for (String ol : outLinks) {
			temp.add(new Text(ol));
		}

		Text[] outLinksArray = temp.toArray(new Text[temp.size()]);
		this.outLinks.set(outLinksArray);
	}

	public PageMetaNode(VIntWritable id, Text title, VIntWritable outCount,
			TextArrayWritable outLinks, FloatWritable score) {
		super(id, outCount, score);
		this.title = title;
		this.outLinks = outLinks;
	}
	
	public void set(VIntWritable id, Text title, VIntWritable outCount,
			TextArrayWritable outLinks, FloatWritable score) {
		super.set(id, outCount, score);
		this.title = title;
		this.outLinks = outLinks;
	}

	public Text getTitle() {
		return title;
	}

	public void setTitle(Text title) {
		this.title = title;
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
		title.write(out);
		outLinks.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		title.readFields(in);
		outLinks.readFields(in);
	}

	@Override
	public int hashCode() {
		return super.hashCode() + getOutLinks().hashCode() + 2
				* getTitle().hashCode();
	}

}
