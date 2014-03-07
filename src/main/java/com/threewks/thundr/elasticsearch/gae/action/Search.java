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
package com.threewks.thundr.elasticsearch.gae.action;

import com.threewks.thundr.http.service.HttpRequest;
import com.threewks.thundr.http.service.HttpResponse;
import org.elasticsearch.index.query.BaseQueryBuilder;

public class Search extends BaseAction {
	private Search() {
		super();
	}

	@Override
	public HttpResponse execute(HttpRequest request) {
		return request.post();
	}

	public static class Builder extends BaseBuilder<Search, Builder> {
		private BaseQueryBuilder queryBuilder;

		public Builder timeout(int millis) {
			parameter("timeout", millis);
			return this;
		}

		public Builder from(int index) {
			parameter("from", index);
			return this;
		}

		public Builder size(int size) {
			parameter("size", size);
			return this;
		}

		public Builder searchType(String type) {
			parameter("search_type", type);
			return this;
		}

		public Builder query(BaseQueryBuilder queryBuilder) {
			this.queryBuilder = queryBuilder;
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
			if (type != null) stringBuilder.append("/" + type);
			stringBuilder.append("/_search");
			return stringBuilder.toString();
		}

		private String buildQuery() {
			return String.format("{\"query\":%s}", queryBuilder.toString());
		}
	}
}
