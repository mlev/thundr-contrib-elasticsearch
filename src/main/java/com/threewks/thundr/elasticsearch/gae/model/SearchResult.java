package com.threewks.thundr.elasticsearch.gae.model;

import com.google.gson.annotations.SerializedName;

public class SearchResult<T> {
	private int took;
	@SerializedName("timed_out")
	private boolean timedOut;
	private Hits<T> hits;

	public int getTook() {
		return took;
	}

	public boolean isTimedOut() {
		return timedOut;
	}

	public Hits<T> getHits() {
		return hits;
	}
}
