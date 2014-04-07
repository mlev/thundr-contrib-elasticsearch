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

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.threewks.thundr.http.service.HttpRequest;
import com.threewks.thundr.http.service.HttpResponse;

public class BulkUpdate extends BaseAction {
	public static Builder create() {
		return new Builder();
	}

	private BulkUpdate() {
		super();
	}

	@Override
	public HttpResponse execute(HttpRequest request) {
		return request.put();
	}

	public static class Builder extends BaseBulkBuilder<BulkUpdate, BulkUpdate.Builder> {

		public Builder refresh() {
			parameter("refresh", true);
			return this;
		}

		@Override
		public BulkUpdate build() {
			BulkUpdate bulkUpdate = new BulkUpdate();
			bulkUpdate.path = path;
			bulkUpdate.parameters = parameters;
			bulkUpdate.data = buildData();
			return bulkUpdate;
		}

		private String buildData() {
			Gson gson = gsonBuilder.create();
			StringBuilder stringBuilder = new StringBuilder();

			for (String id : documents.keySet()) {
				JsonObject opParameters = new JsonObject();
				opParameters.addProperty("_index", index);
				opParameters.addProperty("_type", type);
				opParameters.addProperty("_id", id);

				JsonObject opType = new JsonObject();
				opType.add("update", opParameters);

				stringBuilder.append(opType.toString()).append("\n");

				Object document = documents.get(id);
				String data = (document instanceof String) ? (String) document : gson.toJson(document);
				stringBuilder.append(data).append("\n");
			}

			return stringBuilder.toString();
		}
	}
}
