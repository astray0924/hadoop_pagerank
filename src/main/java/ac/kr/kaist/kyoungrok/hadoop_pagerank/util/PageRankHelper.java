package ac.kr.kaist.kyoungrok.hadoop_pagerank.util;

import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.VIntWritable;

import ac.kr.kaist.kyoungrok.hadoop_pagerank.writable.PageRankNode;

public class PageRankHelper {
	private static final float jumpingFactor = 0.85f;
	private Map<VIntWritable, PageRankNode> graph;
	private int N;

	public PageRankHelper(Map<VIntWritable, PageRankNode> graph) {
		if (graph == null) {
			throw new IllegalArgumentException("The graph must not be null!");
		}

		this.graph = graph;
		this.N = graph.size();
	}

	public void updateGraphScores(Map<VIntWritable, PageRankNode> graph) {
		for (PageRankNode node : graph.values()) {
			float newScore = getNewScore(node);
			node.updateScore(newScore);
		}
	}

	public float getNewScore(PageRankNode node) {
		float newScore = ((1 - jumpingFactor) * ((float) 1 / N))
				+ (jumpingFactor) * calculateOuterFactor(node);

		return newScore;
	}

	private float calculateOuterFactor(PageRankNode node) {
		float score = 0.0f;

		VIntWritable[] linksIds = (VIntWritable[]) node.getInLinks().toArray();
		for (VIntWritable link : linksIds) {
			if (graph.containsKey(link)) {
				PageRankNode nd = graph.get(link);
				float s = nd.getScore().get();
				int c = nd.getOutCount().get();
				score += (float) s / c;
			}

		}

		return score;
	}
}
