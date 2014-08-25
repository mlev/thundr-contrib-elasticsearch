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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.threewks.thundr.elasticsearch.gae.ElasticSearchClient;
import com.threewks.thundr.elasticsearch.gae.ElasticSearchConfig;
import com.threewks.thundr.elasticsearch.gae.action.Action;
import com.threewks.thundr.elasticsearch.gae.action.Delete;
import com.threewks.thundr.elasticsearch.gae.action.Get;
import com.threewks.thundr.elasticsearch.gae.action.Index;
import com.threewks.thundr.elasticsearch.gae.model.ClientResponse;
import com.threewks.thundr.http.service.ning.HttpServiceNing;

public class ElasticSearchClientTest {

	private static ElasticSearchClient elasticSearchClient;
	private static Node node;

	@BeforeClass
	public static void beforeClass() {
		Settings settings = ImmutableSettings.settingsBuilder()
				.put("index.store.type", "memory")
				.put("path.data", "target/elasticsearch/data").build();
		node = NodeBuilder.nodeBuilder().loadConfigSettings(false).local(true).settings(settings).node().start();

		ElasticSearchConfig config = new ElasticSearchConfig();
		config.setUrl("http://localhost:9200");
		elasticSearchClient = new ElasticSearchClient(new HttpServiceNing(), config);
	}

	@AfterClass
	public static void afterClass() {
		if (node != null) {
			node.close();
		}
	}

	@After
	public void after() throws Exception {
		Action action = new Delete.Builder().index("foo").build();
		elasticSearchClient.execute(action);
	}

	@Test
	public void shouldIndexGetAndDeleteDocument() {

		// INDEX
		Index index = new Index.Builder().index("foo").type("bar").id("12345").document(new Bar()).build();
		ClientResponse indexResponse = elasticSearchClient.execute(index);
		assertThat(indexResponse.getJsonResponse().toString(), is("{\"_index\":\"foo\",\"_type\":\"bar\",\"_id\":\"12345\",\"_version\":1,\"created\":true}"));

		// GET
		Get get = new Get.Builder().index("foo").type("bar").id("12345").build();
		ClientResponse getResponse = elasticSearchClient.execute(get);
		assertThat(getResponse.getJsonResponse().toString(), is("{\"_index\":\"foo\",\"_type\":\"bar\",\"_id\":\"12345\",\"_version\":1,\"found\":true,\"_source\":{\"baz\":\"qux\"}}"));
		assertThat(getResponse.getSourceAsType(Bar.class).baz, is("qux"));

		// DELETE
		Delete delete = new Delete.Builder().index("foo").type("bar").id("12345").build();
		ClientResponse deleteResponse = elasticSearchClient.execute(delete);
		assertThat(deleteResponse.getJsonResponse().toString(), is("{\"found\":true,\"_index\":\"foo\",\"_type\":\"bar\",\"_id\":\"12345\",\"_version\":2}"));

		// GET
		ClientResponse getResponse2 = elasticSearchClient.execute(get);
		assertThat(getResponse2.getJsonResponse().toString(), is("{\"_index\":\"foo\",\"_type\":\"bar\",\"_id\":\"12345\",\"found\":false}"));
	}

	private class Bar {
		String baz = "qux";
	}
}
