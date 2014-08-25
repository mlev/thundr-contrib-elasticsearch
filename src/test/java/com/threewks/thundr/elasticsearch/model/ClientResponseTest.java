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
package com.threewks.thundr.elasticsearch.model;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class ClientResponseTest {

	private ClientResponse searchClientResponse;
	private ClientResponse fetchClientResponse;
	private Gson gson;
	private JsonObject searchResponseJson;
	private JsonObject fetchResponseJson;

	@Before
	public void before() throws Exception {

		searchResponseJson = parseJson("/search-response.json");
		fetchResponseJson = parseJson("/fetch-response.json");

		gson = new GsonBuilder().setPrettyPrinting().create();

		searchClientResponse = new ClientResponse(gson, searchResponseJson);
		fetchClientResponse = new ClientResponse(gson, fetchResponseJson);
	}

	@Test
	public void shouldGetRawJsonResponse() {
		assertThat(searchClientResponse.getJsonResponse(), is(searchResponseJson));
	}

	@Test
	public void shouldGetHitsAsList() {
		List<TestUser> hits = searchClientResponse.getHitsAsType(TestUser.class);
		assertThat(hits.size(), is(2));
		assertTestUser(hits.get(0), "1", "Homer", "Simpson");
		assertTestUser(hits.get(1), "2", "Bart", "Simpson");
	}

	@Test
	public void shouldGetTotalNumberOfHits() {
		assertThat(searchClientResponse.getTotalHits(), is(108L));
	}

	@Test
	public void shouldGetSourceAsObject() {
		TestUser user = fetchClientResponse.getSourceAsType(TestUser.class);
		assertTestUser(user, "1", "Homer", "Simpson");
	}

	private void assertTestUser(TestUser user, String id, String firstName, String lastName) {
		assertThat(user.getId(), is(id));
		assertThat(user.getFirstName(), is(firstName));
		assertThat(user.getLastName(), is(lastName));
	}

	private JsonObject parseJson(String jsonFile) {
		JsonParser parser = new JsonParser();
		InputStream inputStream = getClass().getResourceAsStream(jsonFile);
		return parser.parse(new InputStreamReader(inputStream)).getAsJsonObject();
	}

	/**
	 * Test class to convert JSON responses to.
	 */
	private static class TestUser {

		private String id;
		private String firstName;
		private String lastName;

		private TestUser() {
		}

		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}

		public String getFirstName() {
			return firstName;
		}

		public void setFirstName(String firstName) {
			this.firstName = firstName;
		}

		public String getLastName() {
			return lastName;
		}

		public void setLastName(String lastName) {
			this.lastName = lastName;
		}
	}

}
