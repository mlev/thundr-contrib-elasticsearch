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
package com.threewks.thundr.elasticsearch;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;

import com.threewks.thundr.exception.BaseException;
import com.threewks.thundr.http.service.HttpService;
import com.threewks.thundr.injection.InjectorBuilder;
import com.threewks.thundr.injection.UpdatableInjectionContext;

public class ElasticSearchModuleTest {

	private UpdatableInjectionContext injectionContext;
	private ElasticSearchModule module;

	@Rule
	public ExpectedException exception = ExpectedException.none();

	@Before
	public void before() {
		injectionContext = mock(UpdatableInjectionContext.class);
		module = new ElasticSearchModule();
	}

	@Test
	public void shouldThrowExceptionOnStartupIfNoHttpServiceAvailable() {
		exception.expect(BaseException.class);
		exception.expectMessage(is("Unable to find an implementation of com.threewks.thundr.http.service.HttpService"));
		module.configure(injectionContext);
	}

	@Test
	public void shouldInjectElasticSearchConfigAndClientOnStartup() {
		InjectorBuilder injectorBuilder = mock(InjectorBuilder.class);
		when(injectionContext.get(HttpService.class)).thenReturn(mock(HttpService.class));
		when(injectionContext.get(String.class, "elasticSearchUrl")).thenReturn("http://localhost:9200");
		when(injectionContext.inject(any(ElasticSearchConfig.class))).thenReturn(injectorBuilder);
		when(injectionContext.inject(ElasticSearchClient.class)).thenReturn(injectorBuilder);

		module.configure(injectionContext);

		ArgumentCaptor<ElasticSearchConfig> configCaptor = ArgumentCaptor.forClass(ElasticSearchConfig.class);
		verify(injectionContext).inject(configCaptor.capture());
		ElasticSearchConfig config = configCaptor.getValue();
		assertThat(config.getUrl(), is("http://localhost:9200"));
		verify(injectionContext).inject(ElasticSearchClient.class);
	}

}
