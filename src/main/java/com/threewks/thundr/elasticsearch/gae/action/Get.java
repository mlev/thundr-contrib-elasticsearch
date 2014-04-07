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
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.List;

public class Get extends BaseAction {
	public static Builder create() {
		return new Builder();
	}

	private Get() {
		super();
	}

	@Override
	public HttpResponse execute(HttpRequest request) {
		return request.get();
	}

	public static class Builder extends BaseBuilder<Get, Builder> {
		private boolean sourceOnly = false;

		public Builder sourceOnly() {
			sourceOnly = true;
			return this;
		}

		public Builder fields(String... field) {
			return fields(Arrays.asList(field));
		}

		public Builder fields(List<String> fields) {
			parameter("fields", StringUtils.join(fields, ","));
			return this;
		}

		public Builder refresh() {
			parameter("refresh", true);
			return this;
		}

		public Get build() {
			Get get = new Get();
			get.path = buildPath();
			get.parameters = parameters;
			get.data = null;
			return get;
		}

		private String buildPath() {
			StringBuilder stringBuilder = new StringBuilder();
			stringBuilder.append("/" + index);
			if (type != null) stringBuilder.append("/" + type);
			if (id != null) stringBuilder.append("/" + id);
			if (sourceOnly) stringBuilder.append("/_source");
			return stringBuilder.toString();
		}
	}
}
