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
package com.threewks.thundr.elasticsearch.action;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.BaseQueryBuilder;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.threewks.thundr.elasticsearch.model.SortOptions;
import com.threewks.thundr.http.service.HttpRequest;
import com.threewks.thundr.http.service.HttpResponse;
import org.elasticsearch.search.sort.SortBuilder;

public class Search extends BaseAction {
	public static Builder create() {
		return new Builder();
	}

	private Search() {
		super();
	}

	@Override
	public HttpResponse execute(HttpRequest request) {
		return request.post();
	}

	public static class Builder extends BaseBuilder<Search, Builder> {
		private Integer timeout;
		private Integer from;
		private Integer size;
		private String searchType;
		private SortOptions sort = new SortOptions();
		private List<String> fields = new ArrayList<String>();
		private BaseQueryBuilder queryBuilder;
		private String jsonQuery;

		public Builder timeout(int millis) {
			timeout = millis;
			return this;
		}

		public Builder from(int index) {
			from = index;
			return this;
		}

		public Builder size(int size) {
			this.size = size;
			return this;
		}

		public Builder searchType(String type) {
			searchType = type;
			return this;
		}

		public Builder sort(SortBuilder sortBuilder) {
			this.sort.addSort(sortBuilder);
			return this;
		}

		public Builder fields(List<String> fields) {
			this.fields = fields;
			return this;
		}

		public Builder query(BaseQueryBuilder queryBuilder) {
			this.jsonQuery = null;
			this.queryBuilder = queryBuilder;
			return this;
		}

		public Builder query(String jsonQuery) {
			this.queryBuilder = null;
			this.jsonQuery = jsonQuery;
			return this;
		}

		@Override
		public Search build() {
			Search search = new Search();
			search.path = buildPath();
			search.parameters = parameters;
			search.data = buildQuery();
			return search;
		}

		private String buildPath() {
			StringBuilder stringBuilder = new StringBuilder();
			stringBuilder.append("/" + index);
			if (type != null)
				stringBuilder.append("/" + type);
			stringBuilder.append("/_search");
			return stringBuilder.toString();
		}

		private String buildQuery() {
			JsonObject query = new JsonObject();

			if (timeout != null) {
				query.addProperty("timeout", timeout);
			}
			if (from != null) {
				query.addProperty("from", from);
			}
			if (size != null) {
				query.addProperty("size", size);
			}
			if (searchType != null) {
				query.addProperty("search_type", searchType);
			}
			if (sort.hasOptions()) {
				query.add("sort", sort.asJson());
			}
			if (!fields.isEmpty()) {
				JsonArray fieldsArray = new JsonArray();
				for (String field : fields) {
					fieldsArray.add(new JsonPrimitive(field));
				}
				query.add("fields", fieldsArray);
			}

			query.add("query", new JsonParser().parse(getQueryAsJsonString()).getAsJsonObject());

			return query.toString();
		}

		private String getQueryAsJsonString() {
			if (queryBuilder != null) {
				return queryToString(queryBuilder);
			}
			return jsonQuery;
		}
	}

	public static String queryToString(BaseQueryBuilder query) {
		try {
			XContentBuilder builder = new XContentBuilder(JsonXContent.jsonXContent, new BasicBytesStream(4096));
			builder.prettyPrint();
			query.toXContent(builder, ToXContent.EMPTY_PARAMS);
			return builder.string();
		} catch (Exception e) {
			throw new ElasticsearchException("Failed to build query", e);
		}
	}

}
