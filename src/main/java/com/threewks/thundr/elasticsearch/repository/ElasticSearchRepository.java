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
package com.threewks.thundr.elasticsearch.repository;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;

import com.threewks.thundr.elasticsearch.ElasticSearchClient;
import com.threewks.thundr.elasticsearch.action.Action;
import com.threewks.thundr.elasticsearch.action.BulkIndex;
import com.threewks.thundr.elasticsearch.action.Delete;
import com.threewks.thundr.elasticsearch.action.Get;
import com.threewks.thundr.elasticsearch.action.Index;
import com.threewks.thundr.elasticsearch.action.Search;
import com.threewks.thundr.elasticsearch.action.Update;
import com.threewks.thundr.elasticsearch.model.ClientResponse;

public class ElasticSearchRepository<E extends RepositoryEntity> {

	public static final int SearchStartIndex = 0;
	public static final int DefaultResultSizeLimit = 10;

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
		Map<String, Object> documents = new LinkedHashMap<String, Object>();
		for (E entity : entities) {
			documents.put(entity.getId(), entity);
		}

		Action action = new BulkIndex.Builder()
				.index(index)
				.type(typeName)
				.documents(documents)
				.build();
		client.execute(action);
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

	public void deleteAll() {
		Delete delete = new Delete.Builder()
				.index(index)
				.type(typeName)
				.query("*")
				.build();
		client.execute(delete);
	}

	public List<E> search(final String query) {
		return search(query, SearchStartIndex, DefaultResultSizeLimit);
	}

	public List<E> search(final String query, int start, int limit) {
		
		QueryStringQueryBuilder builder = QueryBuilders.queryString(query);
		for (String field : searchFields) {
			builder.field(field);
		}

		Search search = new Search.Builder()
				.index(index)
				.type(typeName)
				.from(start)
				.size(limit)
				.query(builder)
				.build();
		return search(search);
	}

	public List<E> search(Search search) {
		ClientResponse response = client.execute(search);
		return response.getHitsAsType(entityType);
	}

	public ClientResponse executeSearch(Search search) {
		return client.execute(search);
	}
}
