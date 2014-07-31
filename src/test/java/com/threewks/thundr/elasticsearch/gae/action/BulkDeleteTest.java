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

import com.atomicleopard.expressive.Expressive;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class BulkDeleteTest {
	@Test
	public void shouldGenerateValidRequestForSingleDocument() throws Exception {
		BulkDelete bulkDelete = BulkDelete.create().index("foo").type("bar").document("abc123").build();

		assertThat(bulkDelete.getPath(), is("/_bulk"));
		assertThat((String) bulkDelete.getData(), is("{\"delete\":{\"_index\":\"foo\",\"_type\":\"bar\",\"_id\":\"abc123\"}}\n"));
	}

	@Test
	public void shouldGenerateValidRequestForMultipleDocuments() throws Exception {
		BulkDelete bulkDelete = BulkDelete.create()
				.index("foo")
				.type("bar")
				.documents(Expressive.list("abc123", "xyz456"))
				.build();

		assertThat(bulkDelete.getPath(), is("/_bulk"));
		assertThat((String) bulkDelete.getData(), is(
				"{\"delete\":{\"_index\":\"foo\",\"_type\":\"bar\",\"_id\":\"abc123\"}}\n" +
				"{\"delete\":{\"_index\":\"foo\",\"_type\":\"bar\",\"_id\":\"xyz456\"}}\n"));
	}
}
