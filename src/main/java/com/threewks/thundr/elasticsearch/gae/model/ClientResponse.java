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
package com.threewks.thundr.elasticsearch.gae.model;


import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Iterator;
import java.util.List;

public class ClientResponse {

	private final Gson gson;
	private final JsonObject jsonObject;

	public ClientResponse(Gson gson, JsonObject jsonObject) {
		this.gson = gson;
		this.jsonObject = jsonObject;
	}

	/**
	 * Get the raw JSON response returned by the search.
	 *
	 * @return a {@link JsonObject} containing the response.
	 */
	public JsonObject getJsonResponse() {
		return jsonObject;
	}

	/**
	 * Get the total number of hits returned.
	 *
	 * @return the number of hits.
	 */
	public long getTotalHits() {
		if (jsonObject != null && jsonObject.has("hits")) {
			JsonObject hits = jsonObject.getAsJsonObject("hits");
			if (hits != null) {
				return hits.get("total").getAsLong();
			}
		}

		return 0;
	}

	/**
	 * Get the source (original JSON document) as the given type.
	 *
	 * @param type the type to return.
	 * @param <T> generic type.
	 * @return the source object.
	 */
	public <T> T getSourceAsType(Class<T> type) {
		String source = getSourceAsJson();
		return (source == null) ? null : gson.fromJson(getSourceAsJson(), type);
	}

	/**
	 * Get the source (original JSON document) as a list of the given type.
	 *
	 * @param type the type to return in the list.
	 * @param <T> generic type.
	 * @return the source JSON as a list.
	 */
	public <T> List<T> getSourceAsListOfType(Class<T> type) {
		String source = getSourceAsJson();
		if (source == null) {
			return Lists.newArrayList();
		}
		return gson.fromJson(source, new ListOfType<T>(type));
	}

	/**
	 * Get the hits (search results) as a list of the given type..
	 *
	 * @param type the type of objects to return in the list.
	 * @param <T> generic type
	 * @return a list of results.
	 */
	public <T> List<T> getHitsAsType(Class<T> type) {
		List<T> hits = Lists.newArrayList();
		if (!jsonObject.has("hits")) {
			return hits;
		}

		JsonObject jsonHits = jsonObject.get("hits").getAsJsonObject();
		if (jsonHits == null) {
			return hits;
		}

		Iterator<JsonElement> iterator = jsonHits.get("hits").getAsJsonArray().iterator();
		while (iterator.hasNext()) {
			JsonObject hit = iterator.next().getAsJsonObject();
			String json = hit.get("_source").getAsJsonObject().toString();
			hits.add(gson.fromJson(json, type));
		}

		return hits;
	}

	private String getSourceAsJson() {
		JsonElement jsonElement = jsonObject.get("_source");
		return (jsonElement == null) ? null : jsonElement.toString();
	}

	class ListOfType<X> implements ParameterizedType {
		private Class<?> wrapped;

		public ListOfType(Class<X> wrapped) {
			this.wrapped = wrapped;
		}

		public Type[] getActualTypeArguments() {
			return new Type[] {wrapped};
		}

		public Type getRawType() {
			return List.class;
		}

		public Type getOwnerType() {
			return null;
		}
	}

}
