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

import com.google.common.collect.Maps;
import com.google.gson.GsonBuilder;
import com.threewks.thundr.logger.Logger;

import java.util.Map;

@SuppressWarnings("unchecked")
public abstract class BaseBulkBuilder<T, B> extends BaseBuilder<T, B> {
	protected GsonBuilder gsonBuilder = new GsonBuilder();
	protected String path = "/_bulk";
	protected Map<String, Object> documents = Maps.newLinkedHashMap();

	@Override
	public B id(String id) {
		Logger.warn("Setting ID for bulk operations has no effect; ignoring.");
		return (B) this;
	}

	public B document(String id, Object document) {
		this.documents.put(id, document);
		return (B) this;
	}

	public B documents(Map<String, Object> documents) {
		this.documents.putAll(documents);
		return (B) this;
	}
}
