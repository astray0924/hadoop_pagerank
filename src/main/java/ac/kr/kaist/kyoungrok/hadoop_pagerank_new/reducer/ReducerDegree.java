package ac.kr.kaist.kyoungrok.hadoop_pagerank_new.reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import ac.kr.kaist.kyoungrok.hadoop_pagerank_new.writable.VIntArrayWritable;

public class ReducerDegree extends
		Reducer<VIntWritable, VIntWritable, VIntWritable, VIntArrayWritable> {
	private enum LinkScale {
		DEFAULT, TEN, TWENTY, THIRTY, FIFTY, HUNDRED, THOUSAND, EVEN_MORE
	}

	@Override
	public void reduce(VIntWritable id, Iterable<VIntWritable> linksIter,
			Context context) throws IOException, InterruptedException {
		List<VIntWritable> links = new ArrayList<VIntWritable>();
		for (VIntWritable l : linksIter) {
			links.add(new VIntWritable(l.get()));
		}

		// int k = 100;
		// VIntArrayWritable linksWritable = new
		// VIntArrayWritable(links.subList(
		// 0, k > links.size() ? links.size() : k));
		VIntArrayWritable linksWritable = new VIntArrayWritable(links);

		context.write(id, linksWritable);

		// 링크 개수 통계
		int linkScale = links.size();
		LinkScale scale = LinkScale.DEFAULT;
		if (linkScale <= 10) {
			scale = LinkScale.TEN;
		} else if (linkScale <= 20) {
			scale = LinkScale.TWENTY;
		} else if (linkScale <= 30) {
			scale = LinkScale.THIRTY;
		} else if (linkScale <= 50) {
			scale = LinkScale.FIFTY;
		} else if (linkScale <= 100) {
			scale = LinkScale.HUNDRED;
		} else if (linkScale <= 1000) {
			scale = LinkScale.THOUSAND;
		} else {
			scale = LinkScale.EVEN_MORE;
		}

		context.getCounter(scale).increment(1);
	}
}
