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
package com.threewks.thundr.elasticsearch.gae;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.threewks.thundr.elasticsearch.gae.auth.AuthenticationStrategy;
import com.threewks.thundr.elasticsearch.gae.model.*;
import com.threewks.thundr.http.ContentType;
import com.threewks.thundr.http.service.HttpRequest;
import com.threewks.thundr.http.service.HttpResponse;
import com.threewks.thundr.http.service.HttpService;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Map;

public class ElasticSearchClient {
	private final HttpService httpService;
	private final ElasticSearchConfig config;
	private final Gson gson;

	public ElasticSearchClient(HttpService httpService, ElasticSearchConfig config) {
		this.httpService = httpService;
		this.config = config;

		gson = new GsonBuilder().create();
	}

	public <T> Hit<T> get(Class<T> entityType, String path) {
		return get(entityType, path, null);
	}

	public <T> Hit<T> get(Class<T> entityType, String index, String type, String id) {
		return get(entityType, index, type, id, null);
	}

	public <T> Hit<T> get(Class<T> entityType, String index, String type, String id, Map<String, Object> parameters) {
		String path = String.format("/%s/%s/%s", index, type, id);
		return get(entityType, path, parameters);
	}

	public <T> Hit<T> get(Class<T> entityType, String path, Map<String, Object> parameters) {
		String url = String.format("%s%s", config.getUrl(), path);

		HttpRequest request = httpService.request(url);
		applyAuthIfRequired(request);
		applyParametersIfProvided(request, parameters);
		HttpResponse response = request.get();

		return gson.fromJson(response.getBody(), new HitOfType<T>(entityType));
	}

	public IndexResult index(String index, String type, String id, Object data) {
		return index(index, type, id, data, false);
	}

	public IndexResult index(String index, String type, String id, Object data, boolean refresh) {
		String url = String.format("%s/%s/%s/%s", config.getUrl(), index, type, id);

		HttpRequest request = httpService.request(url);
		applyAuthIfRequired(request);
		appendDataToRequest(request, data);
		if (refresh) request.parameter("refresh", true);

		HttpResponse response = request.put();
		return gson.fromJson(response.getBody(), IndexResult.class);
	}

	public IndexResult update(String index, String type, String id, Object data) {
		String url = String.format("%s/%s/%s/%s/_update", config.getUrl(), index, type, id);

		HttpRequest request = httpService.request(url);
		applyAuthIfRequired(request);
		appendDataToRequest(request, data);

		HttpResponse response = request.post();
		return gson.fromJson(response.getBody(), IndexResult.class);
	}

	/**
	 * Delete a document from the index.
	 *
	 * @param index the name of the index the document belongs to
	 * @param type the type of the document
	 * @param id the ID of the document
	 * @return the result
	 */
	public Result delete(String index, String type, String id) {
		String url = String.format("%s/%s/%s/%s", config.getUrl(), index, type, id);

		HttpRequest request = httpService.request(url);
		applyAuthIfRequired(request);

		HttpResponse response = request.delete();
		return gson.fromJson(response.getBody(), Result.class);
	}

	/**
	 * Delete documents from the index matching the given query
	 *
	 * @param index the name of the index the document belongs to
	 * @param type the type of the document
	 * @param query the ID of the document
	 * @return the result
	 */
	public Result deleteByQuery(String index, String type, String query) {
		String url = String.format("%s/%s/%s/_query?q=%s", config.getUrl(), index, type, query);

		HttpRequest request = httpService.request(url);
		applyAuthIfRequired(request);

		HttpResponse response = request.delete();
		return gson.fromJson(response.getBody(), Result.class);
	}

	/**
	 * Deletes an entire index.
	 *
	 * @param index the name of the index to delete
	 * @return the result
	 */
	public Result delete(String index) {
		String url = String.format("%s/%s/", config.getUrl(), index);

		HttpRequest request = httpService.request(url);
		applyAuthIfRequired(request);

		HttpResponse response = request.delete();
		return gson.fromJson(response.getBody(), Result.class);
	}

	public <T> SearchResult<T> search(Class<T> entityType, String index, String type, Query query) {
		String url = String.format("%s/%s/%s/_search", config.getUrl(), index, type);

		HttpRequest request = httpService.request(url);
		applyAuthIfRequired(request);
		appendDataToRequest(request, query);

		HttpResponse response = request.post();
		return gson.fromJson(response.getBody(), new SearchResultOfType<T>(entityType));
	}

	private HttpRequest applyAuthIfRequired(HttpRequest request) {
		AuthenticationStrategy authenticationStrategy = config.getAuthenticationStrategy();
		if (authenticationStrategy != null) {
			request = authenticationStrategy.authenticate(request);
		}
		return request;
	}

	private HttpRequest applyParametersIfProvided(HttpRequest request, Map<String, Object> parameters) {
		if (parameters != null) {
			request = request.parameters(parameters);
		}
		return request;
	}

	private HttpRequest appendDataToRequest(HttpRequest request, Object data) {
		if (data == null) return request;

		String json = gson.toJson(data);
		return request.contentType(ContentType.ApplicationJson).body(json);
	}

	class HitOfType<X> implements ParameterizedType {
		private Class<?> wrapped;

		public HitOfType(Class<X> wrapped) {
			this.wrapped = wrapped;
		}

		public Type[] getActualTypeArguments() {
			return new Type[] {wrapped};
		}

		public Type getRawType() {
			return Hit.class;
		}

		public Type getOwnerType() {
			return null;
		}
	}

	class SearchResultOfType<X> implements ParameterizedType {
		private Class<?> wrapped;

		public SearchResultOfType(Class<X> wrapped) {
			this.wrapped = wrapped;
		}

		public Type[] getActualTypeArguments() {
			return new Type[] {wrapped};
		}

		public Type getRawType() {
			return SearchResult.class;
		}

		public Type getOwnerType() {
			return null;
		}
	}
}
