/*
 * Copyright 2013-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.sleuth.otel.bridge;

import org.springframework.cloud.sleuth.api.Span;
import org.springframework.cloud.sleuth.api.http.HttpClientRequest;
import org.springframework.lang.Nullable;

/**
 * OpenTelemetry wrapper for {@link HttpClientRequest} to pass the stacked contexts.
 *
 * @author Marcin Grzejszczak
 * @since 3.0.0
 */
public class OtelHttpClientRequest implements HttpClientRequest {

	private final OtelTraceContext traceContext;

	private final HttpClientRequest delegate;

	public OtelHttpClientRequest(OtelTraceContext traceContext, HttpClientRequest delegate) {
		this.traceContext = traceContext;
		this.delegate = delegate;
	}

	@Override
	public Span.Kind spanKind() {
		return delegate.spanKind();
	}

	@Override
	public void header(String name, String value) {
		delegate.header(name, value);
	}

	@Override
	public String method() {
		return delegate.method();
	}

	@Override
	@Nullable
	public String path() {
		return delegate.path();
	}

	@Override
	@Nullable
	public String route() {
		return delegate.route();
	}

	@Override
	@Nullable
	public String url() {
		return delegate.url();
	}

	@Override
	@Nullable
	public String header(String name) {
		return delegate.header(name);
	}

	@Override
	public String remoteIp() {
		return delegate.remoteIp();
	}

	@Override
	public int remotePort() {
		return delegate.remotePort();
	}

	@Override
	public Object unwrap() {
		return delegate.unwrap();
	}

	public OtelTraceContext context() {
		return this.traceContext;
	}

}
