package ac.kr.kaist.kyoungrok.hadoop_pagerank_new.util;

import java.util.Map;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.VIntWritable;

import ac.kr.kaist.kyoungrok.hadoop_pagerank_new.writable.PageRankNode;

public class PageRankHelper {
	private static final float jumpingFactor = 0.85f;

	public static void updateGraphScores(Map<VIntWritable, PageRankNode> graph) {
		for (PageRankNode node : graph.values()) {
			float newScore = calculateNewScore(graph, node);
			node.updateScore(newScore);
		}
	}

	public static float calculateNewScore(
			Map<VIntWritable, PageRankNode> graph, PageRankNode info) {
		float nodeCount = (float) graph.size();
		float newScore = ((1 - jumpingFactor) * (1 / nodeCount))
				+ (jumpingFactor) * calculateOuterFactor(graph, info);

		return newScore;
	}

	private static float calculateOuterFactor(
			Map<VIntWritable, PageRankNode> graph, PageRankNode info) {
		float score = 0.0f;

		LongWritable[] linksIds = (LongWritable[]) info.getInLinks().toArray();
		for (LongWritable link : linksIds) {
			if (graph.containsKey(link)) {
				PageRankNode l = graph.get(link);
				float s = Float.parseFloat(l.getScore().toString());
				int c = Integer.parseInt(l.getOutCount().toString());

				score += (float) s / c;
			}

		}

		return score;
	}
}
