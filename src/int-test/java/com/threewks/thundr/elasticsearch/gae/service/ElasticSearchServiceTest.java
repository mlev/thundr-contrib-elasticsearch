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
package com.threewks.thundr.elasticsearch.gae.service;

import com.google.appengine.api.urlfetch.URLFetchService;
import com.google.appengine.api.urlfetch.URLFetchServiceFactory;
import com.google.common.collect.Maps;
import com.threewks.thundr.gae.SetupAppengine;
import com.threewks.thundr.http.service.HttpService;
import com.threewks.thundr.http.service.gae.HttpServiceImpl;
import org.junit.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class ElasticSearchServiceTest {
	private static Process process;

	@Rule
	public SetupAppengine setupAppengine = new SetupAppengine();

	private HttpService httpService;
	private ElasticSearchService elasticSearchService;

	@BeforeClass
	public static void beforeClass() throws URISyntaxException, IOException, InterruptedException {
		/*
		 * WARNING: this is a dirty, dirty hack to get around the fact that App Engine's Lucene jars
		 * conflict with Elastic Search's. This means we can't launch the embedded client which would
		 * make things much, much nicer...sigh.
		 */

		URL resource = ElasticSearchServiceTest.class.getResource("/elasticsearch-1.0.1/bin/elasticsearch");
	 	String command = Paths.get(resource.toURI()).toAbsolutePath().toString();
		ProcessBuilder processBuilder = new ProcessBuilder(command, "-Des.index.store.type=memory");
		process = processBuilder.start();

		System.out.print("Waiting for elastic search instance to start..");
		BufferedReader stdout = new BufferedReader(new InputStreamReader(process.getInputStream()));
		while (!stdout.readLine().endsWith("started")) {
			System.out.print(".");
			Thread.sleep(1000);
		}
		System.out.println(".started.");
	}

	@AfterClass
	public static void afterClass() {
		process.destroy();
	}

	@Before
	public void before() throws Exception {
		URLFetchService urlFetchService = URLFetchServiceFactory.getURLFetchService();
		httpService = new HttpServiceImpl(urlFetchService);

		ElasticSearchConfig config = new ElasticSearchConfig();
		config.setUrl("http://localhost:9200");

		elasticSearchService = new ElasticSearchService(httpService, config);
	}

	@Test
	public void shouldGetResponseFromRoot() throws Exception {
		ElasticSearchResult result = elasticSearchService.get("/");
		assertThat(result, is(notNullValue()));
		assertThat(result.getHttpResponse(), is(notNullValue()));
		assertThat(result, hasKey("status"));
		assertThat((Double) result.get("status"), is(200d));
		assertThat(result.get("version"), is(instanceOf(Map.class)));
	}

	@Test
	public void shouldIndexGetAndDeleteDocument() throws Exception {
		Map<String, Object> data = Maps.newHashMap();
		data.put("baz", "qux");

		String id = UUID.randomUUID().toString();
		ElasticSearchResult result = elasticSearchService.index("foo", "bar", id, data);
		assertThat(result, is(notNullValue()));
		assertThat((String) result.get("_index"), is("foo"));
		assertThat((String) result.get("_type"), is("bar"));
		assertThat((Boolean) result.get("created"), is(true));

		result = elasticSearchService.get("foo", "bar", id);
		assertThat(result, is(notNullValue()));
		assertThat((String) result.get("_id"), is(id));

		Map<String, Object> source = (Map<String, Object> ) result.get("_source");
		assertThat((String) source.get("baz"), is("qux"));
	}
}
