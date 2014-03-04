/*
 * This file is a component of thundr, a software library from 3wks.
 * Read more: http://www.3wks.com.au/thundr
 * Copyright (C) 2013 3wks, <thundr@3wks.com.au>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.threewks.thundr.elasticsearch.gae.repository;

import com.google.common.collect.Lists;
import com.threewks.thundr.elasticsearch.gae.ElasticSearchClient;
import com.threewks.thundr.elasticsearch.gae.model.*;

import java.util.Arrays;
import java.util.List;

public class ElasticSearchRepository<E extends RepositoryEntity> {
	private final ElasticSearchClient client;
	private final String index;
	private final Class<E> entityType;
	private final List<String> searchFields;
	private final String typeName;

	public ElasticSearchRepository(ElasticSearchClient client, String index, Class<E> entityType, List<String> searchFields) {
		this.client = client;
		this.index = index;
		this.entityType = entityType;
		this.searchFields = searchFields;
		typeName = entityType.getSimpleName().toLowerCase();
	}

	public E load(String id) {
		Hit<E> result = client.get(entityType, index, typeName, id);
		return result.getSource();
	}

	public boolean save(E entity) {
		IndexResult result;
		String id = entity.getId();
		if (load(id) == null) {
			result = client.index(index, typeName, id, entity);
		} else {
			result = client.update(index, typeName, id, entity);
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
		client.delete(index, typeName, entity.getId());
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

	public List<E> search(final String query) {
		return search(new Query(new QueryStringQuery(query, searchFields)));
	}

	public List<E> search(Query query) {
		List<E> entities = Lists.newArrayList();
		SearchResult<E> result = client.search(entityType, index, typeName, query);
		for (Hit<E> hit : result.getHits().getHits()) {
			entities.add(hit.getSource());
		}
		return entities;
	}
}
