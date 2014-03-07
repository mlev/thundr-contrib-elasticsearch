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
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class GetTest {
	@Test
	public void shouldBuildAction() throws Exception {
		Get get = new Get.Builder().index("foo").type("bar").id("1").build();
		assertThat(get.getPath(), is("/foo/bar/1"));
		assertThat(get.getParameters().size(), is(0));
		assertThat(get.getData(), is(nullValue()));
	}

	@Test
	public void shouldBuildActionWithParameters() throws Exception {
		Get get = new Get.Builder().index("foo").type("bar").id("1").parameter("test", "hi").build();
		assertThat(get.getPath(), is("/foo/bar/1"));
		assertThat(get.getParameters().size(), is(1));
		assertThat(get.getParameters(), hasEntry("test", (Object) "hi"));
		assertThat(get.getData(), is(nullValue()));
	}

	@Test
	public void shouldBuildActionWithFields() throws Exception {
		Get get = new Get.Builder().index("foo").type("bar").id("1").fields("f1", "f2").build();
		assertThat(get.getPath(), is("/foo/bar/1"));
		assertThat(get.getParameters().size(), is(1));
		assertThat(get.getParameters(), hasEntry("fields", (Object) "f1,f2"));
		assertThat(get.getData(), is(nullValue()));
	}

	@Test
	public void shouldCallRequestGet() throws Exception {
		Get get = new Get.Builder().build();

		HttpRequest request = mock(HttpRequest.class);
		get.execute(request);
		verify(request).get();
	}
}
