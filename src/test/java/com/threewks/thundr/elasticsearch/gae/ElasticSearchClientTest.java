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
package com.threewks.thundr.elasticsearch.gae;

import com.threewks.thundr.elasticsearch.gae.action.Get;
import com.threewks.thundr.elasticsearch.gae.action.Index;
import com.threewks.thundr.elasticsearch.gae.model.ClientResponse;
import com.threewks.thundr.http.service.HttpRequest;
import com.threewks.thundr.http.service.HttpResponse;
import com.threewks.thundr.http.service.HttpService;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.*;

public class ElasticSearchClientTest {
	public static final String BaseUrl = "http://localhost:9200";
	private ElasticSearchClient client;
	private HttpService httpService;
	private HttpRequest request;

	@Before
	public void before() throws Exception {
		request = mock(HttpRequest.class, RETURNS_DEEP_STUBS);
		doReturn(request).when(request).parameters(anyMap());
		doReturn(request).when(request).body(anyObject());

		httpService = mock(HttpService.class);
		doReturn(request).when(httpService).request(anyString());

		ElasticSearchConfig config = new ElasticSearchConfig();
		config.setUrl(BaseUrl);

		client = new ElasticSearchClient(httpService, config);
	}

	@Test
	public void shouldGetDocument() throws Exception {
		HttpResponse response = mock(HttpResponse.class);
		when(response.getBody()).thenReturn("{" +
				"   \"_index\": \"foo\"," +
				"   \"_type\": \"bar\"," +
				"   \"_id\": \"1\"," +
				"   \"_version\": 1," +
				"   \"found\": true," +
				"   \"_source\": {" +
				"      \"baz\": \"qux\"" +
				"   }" +
				"}");
		when(request.get()).thenReturn(response);

		Get get = new Get.Builder()
				.index("foo")
				.type("bar")
				.id("1")
				.build();
		ClientResponse clientResponse = client.execute(get);
		verify(httpService).request(BaseUrl + "/foo/bar/1");
		verify(request, times(0)).body(anyObject());

		Foo foo = clientResponse.getSourceAsType(Foo.class);
		assertThat(foo, is(notNullValue()));
		assertThat(foo.baz, is("qux"));
	}

	@Test
	@Ignore
	public void shouldIndexDocument() throws Exception {
		HttpResponse response = mock(HttpResponse.class);
		when(response.getBody()).thenReturn("{" +
				"   \"_index\": \"foo\"," +
				"   \"_type\": \"bar\"," +
				"   \"_id\": \"1\"," +
				"   \"_version\": 1," +
				"   \"found\": true," +
				"   \"_source\": {" +
				"      \"baz\": \"qux\"" +
				"   }" +
				"}");
		when(request.get()).thenReturn(response);

		Index index = new Index.Builder()
				.index("foo")
				.type("bar")
				.id("1")
				.document(new Foo())
				.build();
		ClientResponse clientResponse = client.execute(index);
		verify(httpService).request(BaseUrl + "/foo/bar/1");
		verify(request, times(0)).body(anyObject());

		Foo foo = clientResponse.getSourceAsType(Foo.class);
		assertThat(foo, is(notNullValue()));
		assertThat(foo.baz, is("qux"));
	}

	public class Foo {
		public String baz;
	}
}
