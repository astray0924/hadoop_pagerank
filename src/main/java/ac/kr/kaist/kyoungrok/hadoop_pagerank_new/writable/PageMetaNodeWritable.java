package ac.kr.kaist.kyoungrok.hadoop_pagerank_new.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;

public class PageMetaNodeWritable extends AbstractPageNodeWritable {
	private Text title;
	private TextArrayWritable outLinks;

	@Override
	public String toString() {
		return String
				.format("ID: %s, TITLE: %s, OUTLINKS SIZE: %s, OUTCOUNT: %s, SCORE: %f",
						id, title, outLinks.getSize(), outCount, score);
	}

	public PageMetaNodeWritable() {
		super();
		this.title = new Text("");
		this.outLinks = new TextArrayWritable();
	}

	public PageMetaNodeWritable(Integer id, String title,
			List<String> outLinks, Integer outCount, Float score) {
		super(id, outCount, score);

		// title
		this.title = new Text(title);

		// Out Links
		Text[] outLinksArray = outLinks.toArray(new Text[] { new Text("") });
		this.outLinks.set(outLinksArray);
	}

	public PageMetaNodeWritable(VIntWritable id, Text title,
			VIntWritable outCount, TextArrayWritable outLinks,
			FloatWritable score) {
		super(id, outCount, score);
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
