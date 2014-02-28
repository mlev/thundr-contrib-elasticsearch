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
package com.threewks.thundr.elasticsearch.gae.service;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.threewks.thundr.elasticsearch.gae.auth.AuthenticationStrategy;
import com.threewks.thundr.http.ContentType;
import com.threewks.thundr.http.service.HttpRequest;
import com.threewks.thundr.http.service.HttpResponse;
import com.threewks.thundr.http.service.HttpService;

import java.util.Map;

public class ElasticSearchService {
	private final HttpService httpService;
	private final ElasticSearchConfig config;
	private final GsonBuilder gsonBuilder;

	public ElasticSearchService(HttpService httpService, ElasticSearchConfig config) {
		this.httpService = httpService;
		this.config = config;

		gsonBuilder = new GsonBuilder();
	}

	public ElasticSearchResult get(String path) {
		return get(path, null);
	}

	public ElasticSearchResult get(String path, Map<String, Object> parameters) {
		String url = String.format("%s%s", config.getUrl(), path);

		HttpRequest request = httpService.request(url);
		applyAuthIfRequired(request);
		applyParametersIfProvided(request, parameters);
		HttpResponse response = request.get();
		return parseResponse(response);
	}

	public ElasticSearchResult get(String index, String type, String id) {
		return get(index, type, id, null);
	}

	public ElasticSearchResult get(String index, String type, String id, Map<String, Object> parameters) {
		String path = String.format("/%s/%s/%s", index, type, id);
		return get(path, parameters);
	}

	public ElasticSearchResult index(String index, String type, String id, Object data) {
		String url = String.format("%s/%s/%s/%s", config.getUrl(), index, type, id);

		HttpRequest request = httpService.request(url);
		applyAuthIfRequired(request);
		appendDataToRequest(request, data);

		HttpResponse response = request.put();
		return parseResponse(response);
	}

	public ElasticSearchResult update(String index, String type, String id, Object data) {
		String url = String.format("%s/%s/%s/%s/_update", config.getUrl(), index, type, id);

		HttpRequest request = httpService.request(url);
		applyAuthIfRequired(request);
		appendDataToRequest(request, data);

		HttpResponse response = request.post();
		return parseResponse(response);
	}

	public ElasticSearchResult delete(String index, String type, String id) {
		String url = String.format("%s/%s/%s/%s", config.getUrl(), index, type, id);

		HttpRequest request = httpService.request(url);
		applyAuthIfRequired(request);

		HttpResponse response = request.delete();
		return parseResponse(response);
	}

	public ElasticSearchResult search(String index, String type, Map<String, Object> query) {
		return  searchInternal(index, type, query, false);
	}

	public ElasticSearchResult advancedSearch(String index, String type, Map<String, Object> query) {
		return  searchInternal(index, type, query, true);
	}

	private ElasticSearchResult searchInternal(String index, String type, Map<String, Object> query, boolean advanced) {
		String url = String.format("%s/%s/%s/_search", config.getUrl(), index, type);

		HttpRequest request = httpService.request(url);
		applyAuthIfRequired(request);

		if (advanced) {
			appendDataToRequest(request, query);
		} else {
			request.parameters(query);
		}

		HttpResponse response = request.get();
		return parseResponse(response);
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
		Gson gson = gsonBuilder.create();
		String json = gson.toJson(data);
		return request.contentType(ContentType.ApplicationJson).body(json);
	}

	private ElasticSearchResult parseResponse(HttpResponse response) {
		Gson gson = gsonBuilder.create();
		ElasticSearchResult result = gson.fromJson(response.getBody(), ElasticSearchResult.class);
		result.httpResponse = response;
		return result;
	}
}
