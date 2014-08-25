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

import com.threewks.thundr.http.service.HttpRequest;
import com.threewks.thundr.http.service.HttpResponse;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

public class Index extends BaseAction {
	protected boolean idSet = false;

	public static Builder create() {
		return new Builder();
	}

	private Index() {
		super();
	}

	@Override
	public HttpResponse execute(HttpRequest request) {
		return (idSet) ? request.put() : request.post();  // auto-generate ID if not set via POST
	}

	public static class Builder extends BaseBuilder<Index, Builder> {
		private static final DateTimeFormatter isoFormat = ISODateTimeFormat.dateTime();

		private Object document;

		public Builder document(Object document) {
			this.document = document;
			return this;
		}

		public Builder version(String version) {
			parameter("version", version);
			return this;
		}

		public Builder versionType(String type) {
			parameter("version_type", type);
			return this;
		}

		public Builder operationType(String type) {
			parameter("operation_type", type);
			return this;
		}

		public Builder parent(String id) {
			parameter("parent", id);
			return this;
		}

		public Builder timestamp(DateTime dateTime) {
			parameter("timestamp", isoFormat.print(dateTime));
			return this;
		}

		public Builder ttl(String value) {
			parameter("ttl", value);
			return this;
		}

		public Builder consistency(String value) {
			parameter("consistency", value);
			return this;
		}

		public Builder replication(String value) {
			parameter("replication", value);
			return this;
		}

		public Builder refresh() {
			parameter("refresh", true);
			return this;
		}

		public Builder timeout(String value) {
			parameter("timeout", value);
			return this;
		}

		@Override
		public Index build() {
			Index index = new Index();
			index.path = buildPath();
			index.parameters = parameters;
			index.data = document;
			index.idSet = !StringUtils.isEmpty(id);
			return index;
		}

		private String buildPath() {
			StringBuilder stringBuilder = new StringBuilder();
			stringBuilder.append("/" + index);
			if (type != null) stringBuilder.append("/" + type);
			if (id != null) stringBuilder.append("/" + id);
			return stringBuilder.toString();
		}
	}
}
