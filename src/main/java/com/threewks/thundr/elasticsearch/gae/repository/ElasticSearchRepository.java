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

import com.threewks.thundr.elasticsearch.gae.ElasticSearchClient;
import com.threewks.thundr.elasticsearch.gae.action.*;
import com.threewks.thundr.elasticsearch.gae.model.ClientResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;

import java.util.Arrays;
import java.util.List;

public class ElasticSearchRepository<E extends RepositoryEntity> {
	protected final ElasticSearchClient client;
	protected final String index;
	protected final Class<E> entityType;
	protected final List<String> searchFields;
	protected final String typeName;

	public ElasticSearchRepository(ElasticSearchClient client, String index, Class<E> entityType, List<String> searchFields) {
		this.client = client;
		this.index = index;
		this.entityType = entityType;
		this.searchFields = searchFields;
		typeName = entityType.getSimpleName().toLowerCase();
	}

	public E load(String id) {
		Get get = new Get.Builder()
				.index(index)
				.type(typeName)
				.id(id)
				.build();
		ClientResponse response = client.execute(get);
		return response.getSourceAsType(entityType);
	}

	public boolean save(E entity) {
		String id = entity.getId();

		Action action;
		if (load(id) == null) {
			action = new Index.Builder()
					.index(index)
					.type(typeName)
					.id(id)
					.document(entity)
					.build();
		} else {
			action = new Update.Builder()
					.index(index)
					.type(typeName)
					.id(id)
					.document(entity)
					.build();
		}
		client.execute(action);
		return action instanceof Index;
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
		Delete delete = new Delete.Builder()
				.index(index)
				.type(typeName)
				.id(entity.getId())
				.build();
		client.execute(delete);
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
		QueryStringQueryBuilder builder = QueryBuilders.queryString(query);
		for (String field : searchFields) {
			builder.field(field);
		}

		Search search = new Search.Builder()
				.index(index)
				.type(typeName)
				.query(builder)
				.build();
		return search(search);
	}

	public List<E> search(Search search) {
		ClientResponse response = client.execute(search);
		return response.getHitsAsType(entityType);
	}
}
