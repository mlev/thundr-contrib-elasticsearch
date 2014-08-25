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
package com.threewks.thundr.elasticsearch;

import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.join;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.threewks.thundr.elasticsearch.action.Action;
import com.threewks.thundr.elasticsearch.auth.AuthenticationStrategy;
import com.threewks.thundr.elasticsearch.model.ClientResponse;
import com.threewks.thundr.http.ContentType;
import com.threewks.thundr.http.service.HttpRequest;
import com.threewks.thundr.http.service.HttpResponse;
import com.threewks.thundr.http.service.HttpService;

public class ElasticSearchClient {
	private final HttpService httpService;
	private final ElasticSearchConfig config;
	private final GsonBuilder gsonBuilder;
	private JsonParser jsonParser = new JsonParser();

	public ElasticSearchClient(HttpService httpService, ElasticSearchConfig config) {
		this.httpService = httpService;
		this.config = config;

		gsonBuilder = new GsonBuilder();
	}

	/**
	 * Retrieve the GsonBuilder used to construct the ClientResponse Gson object. This is useful
	 * for registering custom type adapters for serialization/deserialization.
	 *
	 * @return a GsonBuilder
	 */
	public GsonBuilder getGsonBuilder() {
		return gsonBuilder;
	}

	public ClientResponse execute(Action action) {
		String url = format("%s%s", config.getUrl(), action.getPath());
		url = applyParametersIfProvided(url, action.getParameters());

		HttpRequest request = httpService.request(url);
		applyAuthIfRequired(request);
		appendDataToRequest(request, action.getData());

		HttpResponse response = action.execute(request);
		return newSearchResponse(response.getBody());
	}

	private HttpRequest applyAuthIfRequired(HttpRequest request) {
		AuthenticationStrategy authenticationStrategy = config.getAuthenticationStrategy();
		if (authenticationStrategy != null) {
			request = authenticationStrategy.authenticate(request);
		}
		return request;
	}

	private String applyParametersIfProvided(String url, Map<String, Object> parameters) {
		if (parameters == null || parameters.size() == 0) {
			return url;
		}

		List<String> queryString = new ArrayList<String>();
		for (String key : parameters.keySet()) {
			queryString.add(format("%s=%s", key, String.valueOf(parameters.get(key))));
		}

		return url + "?" + join(queryString, "&");
	}

	private HttpRequest appendDataToRequest(HttpRequest request, Object data) {
		if (data == null) return request;

		if (!(data instanceof String)) {
			data = gsonBuilder.create().toJson(data);
		}
		return request.contentType(ContentType.ApplicationJson).body(data);
	}

	private ClientResponse newSearchResponse(String json) {
		return new ClientResponse(gsonBuilder.create(), parseResult(json));
	}

	private JsonObject parseResult(String json) {
		return jsonParser.parse(json).getAsJsonObject();
	}
}
