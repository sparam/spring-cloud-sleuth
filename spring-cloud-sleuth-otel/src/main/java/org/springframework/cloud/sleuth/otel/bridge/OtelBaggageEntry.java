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

import io.opentelemetry.baggage.Baggage;
import io.opentelemetry.baggage.BaggageUtils;
import io.opentelemetry.baggage.Entry;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.ContextStorageProvider;
import io.opentelemetry.trace.Span;

import org.springframework.cloud.sleuth.api.BaggageEntry;
import org.springframework.cloud.sleuth.api.CurrentTraceContext;
import org.springframework.cloud.sleuth.api.TraceContext;
import org.springframework.cloud.sleuth.autoconfig.SleuthBaggageProperties;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationEventPublisher;

/**
 * OpenTelemetry implementation of a {@link BaggageEntry}.
 *
 * @author Marcin Grzejszczak
 * @since 3.0.0
 */
public class OtelBaggageEntry implements BaggageEntry {

	private final CurrentTraceContext currentTraceContext;

	private final ContextStorageProvider contextStorageProvider;

	private final ApplicationEventPublisher publisher;

	private final SleuthBaggageProperties sleuthBaggageProperties;

	private final Entry entry;

	public OtelBaggageEntry(CurrentTraceContext currentTraceContext, ContextStorageProvider contextStorageProvider,
			ApplicationEventPublisher publisher, SleuthBaggageProperties sleuthBaggageProperties, Entry entry) {
		this.currentTraceContext = currentTraceContext;
		this.contextStorageProvider = contextStorageProvider;
		this.publisher = publisher;
		this.sleuthBaggageProperties = sleuthBaggageProperties;
		this.entry = entry;
	}

	@Override
	public String name() {
		return this.entry.getKey();
	}

	@Override
	public String get() {
		return BaggageUtils.getCurrentBaggage().getEntryValue(this.entry.getKey());
	}

	@Override
	public String get(TraceContext traceContext) {
		try (CurrentTraceContext.Scope scope = this.currentTraceContext.maybeScope(traceContext)) {
			return get();
		}
	}

	@Override
	public void set(String value) {
		io.opentelemetry.baggage.Baggage baggage = Baggage.builder()
				.put(this.entry.getKey(), value, this.entry.getEntryMetadata()).build();
		if (this.sleuthBaggageProperties.getTagFields().stream().map(String::toLowerCase)
				.anyMatch(s -> s.equals(this.entry.getKey()))) {
			Span.current().setAttribute(this.entry.getKey(), value);
		}
		this.publisher.publishEvent(new BaggageChanged(this, baggage, this.entry.getKey(), value));
		this.contextStorageProvider.get().attach(Context.current().with(baggage));
	}

	@Override
	public void set(TraceContext traceContext, String value) {
		try (CurrentTraceContext.Scope scope = this.currentTraceContext.maybeScope(traceContext)) {
			set(value);
		}
	}

	public static class BaggageChanged extends ApplicationEvent {

		/**
		 * Baggage with the new entry.
		 */
		public Baggage baggage;

		/**
		 * Baggage entry name.
		 */
		public String name;

		/**
		 * Baggage entry value.
		 */
		public String value;

		public BaggageChanged(OtelBaggageEntry source, Baggage baggage, String name, String value) {
			super(source);
			this.baggage = baggage;
			this.name = name;
			this.value = value;
		}

		@Override
		public String toString() {
			return "BaggageChanged{" + "name='" + name + '\'' + ", value='" + value + '\'' + '}';
		}

	}

}
