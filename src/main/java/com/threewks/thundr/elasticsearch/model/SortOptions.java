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
package com.threewks.thundr.elasticsearch.model;

import java.util.ArrayList;
import java.util.List;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.threewks.thundr.elasticsearch.action.BasicBytesStream;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.search.sort.SortBuilder;

public class SortOptions {

	private List<SortBuilder> options = new ArrayList<SortBuilder>();

	public SortOptions() {
	}

	public SortOptions addSort(SortBuilder builder) {
		options.add(builder);
		return this;
	}

	public boolean hasOptions() {
		return !options.isEmpty();
	}

	public JsonElement asJson() {
		JsonArray optionsArray = new JsonArray();
		JsonParser jsonParser = new JsonParser();
		for (SortBuilder builder : options) {
			optionsArray.add(jsonParser.parse(optionToString(builder)).getAsJsonObject());
		}
		return optionsArray;
	}

	public static String optionToString(ToXContent query) {
		try {
			XContentBuilder builder = new XContentBuilder(JsonXContent.jsonXContent, new BasicBytesStream(4096));
			builder.prettyPrint();
			builder.startObject();
			query.toXContent(builder, ToXContent.EMPTY_PARAMS);
			builder.endObject();
			return builder.string();
		} catch (Exception e) {
			throw new ElasticsearchException("Failed to build query", e);
		}
	}
}
