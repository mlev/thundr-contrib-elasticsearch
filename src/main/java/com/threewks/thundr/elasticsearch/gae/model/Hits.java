package com.threewks.thundr.elasticsearch.gae.model;

import com.google.gson.annotations.SerializedName;

import java.util.List;

public class Hits<T> {
	private int total;
	@SerializedName("max_score")
	private double maxScore;
	private List<Hit<T>> hits;

	public int getTotal() {
		return total;
	}

	public double getMaxScore() {
		return maxScore;
	}

	public List<Hit<T>> getHits() {
		return hits;
	}
}
