package com.threewks.thundr.elasticsearch.gae.repository;

public interface RepositoryEntity {
	String getIndex();
	String getType();
	String getId();
}
