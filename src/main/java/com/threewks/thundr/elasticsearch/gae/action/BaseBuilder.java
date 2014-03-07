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

import com.google.appengine.repackaged.com.google.common.base.StringUtil;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

public abstract class BaseBuilder<T, B> {
	protected String index;
	protected Object type;
	protected String id;
	protected Map<String, Object> parameters = Maps.newHashMap();

	public B index(String index) {
		this.index = index;
		return (B) this;
	}

	public B type(String type) {
		this.type = type;
		return (B) this;
	}

	public B id(String id) {
		this.id = id;
		return (B) this;
	}

	public B parameter(String name, Object value) {
		parameters.put(name, value);
		return (B) this;
	}

	public B parameters(Map<String, Object> parameters) {
		this.parameters.putAll(parameters);
		return (B) this;
	}

	public B routing(List<String> routes) {
		this.parameters.put("routing", StringUtils.join(routes, ","));
		return (B) this;
	}

	public abstract T build();
}
