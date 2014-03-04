package com.threewks.thundr.elasticsearch.gae.model;

public class Hit<T> extends Result {
	private double _score;
	private T _source;

	public double getScore() {
		return _score;
	}

	public T getSource() {
		return _source;
	}
}
