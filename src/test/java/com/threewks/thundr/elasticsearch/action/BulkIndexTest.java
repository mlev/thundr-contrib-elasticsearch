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

import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class BulkIndexTest {

	@Test
	public void shouldGenerateBulkInsertForSingleItem() throws Exception {
		BulkIndex bulkIndex = BulkIndex.create()
				.index("foo")
				.type("bar")
				.document("1", "{\"baz\":123}")
				.build();

		assertThat(bulkIndex.getPath(), is("/_bulk"));
		assertThat((String) bulkIndex.getData(), is(
				"{\"index\":{\"_index\":\"foo\",\"_type\":\"bar\",\"_id\":\"1\"}}\n" +
				"{\"baz\":123}\n"));
	}

	@Test
	public void shouldGenerateBulkInsertForMultipleItems() throws Exception {
		BulkIndex bulkIndex = BulkIndex.create()
				.index("foo")
				.type("bar")
				.document("1", "{\"baz\":123}")
				.document("2", "{\"qux\":456}")
				.build();

		assertThat(bulkIndex.getPath(), is("/_bulk"));
		assertThat((String) bulkIndex.getData(), is(
				"{\"index\":{\"_index\":\"foo\",\"_type\":\"bar\",\"_id\":\"1\"}}\n" +
				"{\"baz\":123}\n" +
				"{\"index\":{\"_index\":\"foo\",\"_type\":\"bar\",\"_id\":\"2\"}}\n" +
				"{\"qux\":456}\n"));
	}
}
