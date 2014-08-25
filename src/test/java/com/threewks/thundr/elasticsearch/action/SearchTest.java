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

import org.elasticsearch.index.query.QueryBuilders;
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class SearchTest {
	@Test
	public void shouldBuildSimpleSearch() throws Exception {
		Search search = Search.create()
				.index("foo")
				.type("bar")
				.query(QueryBuilders.termQuery("hello", "world"))
				.build();

		assertThat(search.getPath(), is("/foo/bar/_search"));
		assertThat((String) search.getData(), is("{\"query\":{\"term\":{\"hello\":\"world\"}}}"));
	}

	@Test
	public void shouldBuildSimpleSearchWithPagingParameters() throws Exception {
		Search search = Search.create()
				.from(0)
				.size(100)
				.query(QueryBuilders.termQuery("hello", "world"))
				.build();

		assertThat((String) search.getData(), is("{\"from\":0,\"size\":100,\"query\":{\"term\":{\"hello\":\"world\"}}}"));
	}

	@Test
	public void shouldBuildSimpleSearchWithFields() throws Exception {
		Search search = Search.create()
				.query(QueryBuilders.termQuery("hello", "world"))
				.fields(Arrays.asList("field1", "field2"))
				.build();

		assertThat((String) search.getData(), is("{\"fields\":[\"field1\",\"field2\"],\"query\":{\"term\":{\"hello\":\"world\"}}}"));
	}
}
