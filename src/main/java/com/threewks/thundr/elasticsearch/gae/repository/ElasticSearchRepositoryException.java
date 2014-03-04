package com.threewks.thundr.elasticsearch.gae.repository;

import com.threewks.thundr.exception.BaseException;

public class ElasticSearchRepositoryException extends BaseException {
	public ElasticSearchRepositoryException(Throwable cause, String format, Object... formatArgs) {
		super(cause, format, formatArgs);
	}

	public ElasticSearchRepositoryException(String format, Object... formatArgs) {
		super(format, formatArgs);
	}
}
