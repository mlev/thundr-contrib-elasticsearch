package com.threewks.thundr.elasticsearch.gae.action;

import com.google.gson.JsonObject;
import com.threewks.thundr.http.service.HttpRequest;
import com.threewks.thundr.http.service.HttpResponse;

import java.util.List;

public class BulkDelete extends BaseAction {

	public static Builder create() {
		return new Builder();
	}

	private BulkDelete() {
		super();
	}

	@Override
	public HttpResponse execute(HttpRequest request) {
		return request.delete();
	}

	public static class Builder extends BaseBulkBuilder<BulkDelete, BulkDelete.Builder> {

		public Builder document(String id) {
			document(id, null);
			return this;
		}

		public Builder documents(List<String> ids) {
			for (String id : ids) {
				document(id, null);
			}
			return this;
		}

		@Override
		public BulkDelete build() {
			BulkDelete bulkDelete = new BulkDelete();
			bulkDelete.path = path;
			bulkDelete.parameters = parameters;
			bulkDelete.data = buildData();
			return bulkDelete;
		}

		private String buildData() {
			StringBuilder stringBuilder = new StringBuilder();

			for (String id : documents.keySet()) {
				JsonObject opParameters = new JsonObject();
				opParameters.addProperty("_index", index);
				opParameters.addProperty("_type", type);
				opParameters.addProperty("_id", id);

				JsonObject opType = new JsonObject();
				opType.add("delete", opParameters);

				stringBuilder.append(opType.toString()).append("\n");
			}

			return stringBuilder.toString();
		}
	}
}
