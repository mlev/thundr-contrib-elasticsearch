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
