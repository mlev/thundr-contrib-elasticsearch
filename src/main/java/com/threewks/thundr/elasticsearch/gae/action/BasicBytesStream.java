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

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.BytesStream;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class BasicBytesStream extends StreamOutput implements BytesStream {
	protected ByteArrayOutputStream bytes;

	public BasicBytesStream(int expectedSize) {
		bytes = new ByteArrayOutputStream(expectedSize);
	}

	@Override
	public void writeByte(byte b) throws IOException {
		bytes.write(b);
	}

	@Override
	public void writeBytes(byte[] b, int offset, int length) throws IOException {
		bytes.write(b, offset, length);
	}

	public void reset() {
		bytes.reset();
	}

	@Override
	public void flush() throws IOException {
		bytes.flush();
	}

	@Override
	public void close() throws IOException {
		bytes.close();
	}

	public int size() {
		return bytes.size();
	}

	@Override
	public BytesReference bytes() {
		return new BytesArray(bytes.toByteArray());
	}
}
