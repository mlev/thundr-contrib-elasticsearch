package com.threewks.thundr.elasticsearch.gae.repository;

import com.google.common.collect.Lists;
import com.threewks.thundr.elasticsearch.gae.ElasticSearchClient;
import com.threewks.thundr.elasticsearch.gae.model.Hit;
import com.threewks.thundr.elasticsearch.gae.model.IndexResult;
import com.threewks.thundr.elasticsearch.gae.model.Query;
import com.threewks.thundr.elasticsearch.gae.model.SearchResult;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ElasticSearchRepository<E extends RepositoryEntity> {
	private final ElasticSearchClient client;
	private final String index;
	private final Class<E> entityType;

	public ElasticSearchRepository(ElasticSearchClient client, String index, Class<E> entityType) {
		this.client = client;
		this.index = index;
		this.entityType = entityType;
	}

	public E load(String id) {
		Hit<E> result = client.get(entityType, index, entityType.getName(), id);
		return result.getSource();
	}

	public boolean save(E entity) {
		IndexResult result;
		String id = entity.getId();
		String type = entityType.getName();
		if (load(id) == null) {
			result = client.index(index, type, id, entity);
		} else {
			result = client.update(index, type, id, entity);
		}
		return result.isCreated();
	}

	public void save(E... entities) {
		save(Arrays.asList(entities));
	}

	public void save(List<E> entities) {
		// TODO use bulk API for better performance
		for (E entity : entities) {
			save(entity);
		}
	}

	public void delete(E entity) {
		client.delete(index, entityType.getName(), entity.getId());
	}

	public void delete(E... entities) {
		Arrays.asList(entities);
	}

	public void delete(List<E> entities) {
		// TODO use bulk API for better performance
		for (E entity : entities) {
			delete(entity);
		}
	}

	public List<E> search(Map<String, Object> query) {
		List<E> entities = Lists.newArrayList();

		Query elasticSearchQuery = new Query(query);
		SearchResult<E> result = client.search(entityType, index, entityType.getName(), elasticSearchQuery);
		for (Hit<E> hit : result.getHits().getHits()) {
			entities.add(hit.getSource());
		}
		return entities;
	}
}
