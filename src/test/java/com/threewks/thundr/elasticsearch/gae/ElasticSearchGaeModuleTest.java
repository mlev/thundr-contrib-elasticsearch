package com.threewks.thundr.elasticsearch.gae;

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

public class ElasticSearchGaeModuleTest {

	private UpdatableInjectionContext injectionContext;
	private ElasticSearchGaeModule module;

	@Rule
	public ExpectedException exception = ExpectedException.none();

	@Before
	public void before() {
		injectionContext = mock(UpdatableInjectionContext.class);
		module = new ElasticSearchGaeModule();
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
