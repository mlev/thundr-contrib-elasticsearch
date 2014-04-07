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

public class Update extends BaseAction {
	public static Builder create() {
		return new Builder();
	}

	private Update() {
		super();
	}

	@Override
	public HttpResponse execute(HttpRequest request) {
		return request.post();
	}

	public static class Builder extends BaseBuilder<Update, Builder> {
		private Object document;

		public Builder document(Object document) {
			this.document = document;
			return this;
		}

		@Override
		public Update build() {
			Update update = new Update();
			update.path = buildPath();
			update.parameters = parameters;
			update.data = document;
			return update;
		}

		private String buildPath() {
			StringBuilder stringBuilder = new StringBuilder();
			stringBuilder.append("/" + index);
			if (type != null) stringBuilder.append("/" + type);
			if (id == null) throw new ActionBuilderException("ID missing from Update action. Did you forget to set it?");
			stringBuilder.append("/" + id);
			stringBuilder.append("/_update");
			return stringBuilder.toString();
		}
	}
}
