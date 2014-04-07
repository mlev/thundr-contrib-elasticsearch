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

public class Delete extends BaseAction {
	public static Builder create() {
		return new Builder();
	}

	private Delete() {
		super();
	}

	@Override
	public HttpResponse execute(HttpRequest request) {
		return request.delete();
	}

	public static class Builder extends BaseBuilder<Delete, Builder> {
		private String query;

		public Builder query(String query) {
			this.query = query;
			parameter("q", query);
			return this;
		}

		@Override
		public Delete build() {
			Delete delete = new Delete();
			delete.path = buildPath();
			delete.parameters = parameters;
			delete.data = null;
			return delete;
		}

		private String buildPath() {
			StringBuilder stringBuilder = new StringBuilder();
			stringBuilder.append("/" + index);
			if (type != null) stringBuilder.append("/" + type);
			if (id != null) {
				stringBuilder.append("/" + id);
			} else if (query != null) {
				stringBuilder.append("/_query");
			}
			return stringBuilder.toString();
		}
	}
}
