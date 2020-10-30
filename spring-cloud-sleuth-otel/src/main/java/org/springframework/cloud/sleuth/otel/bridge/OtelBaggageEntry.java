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

import java.util.concurrent.atomic.AtomicReference;

import io.opentelemetry.api.baggage.Baggage;
import io.opentelemetry.api.baggage.Entry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.ContextStorageProvider;

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

	private final OtelBaggageManager otelBaggageManager;

	private final ContextStorageProvider contextStorageProvider;

	private final CurrentTraceContext currentTraceContext;

	private final ApplicationEventPublisher publisher;

	private final SleuthBaggageProperties sleuthBaggageProperties;

	private final AtomicReference<Entry> entry = new AtomicReference<>();

	public OtelBaggageEntry(OtelBaggageManager otelBaggageManager, ContextStorageProvider contextStorageProvider,
			CurrentTraceContext currentTraceContext, ApplicationEventPublisher publisher,
			SleuthBaggageProperties sleuthBaggageProperties, Entry entry) {
		this.otelBaggageManager = otelBaggageManager;
		this.contextStorageProvider = contextStorageProvider;
		this.currentTraceContext = currentTraceContext;
		this.publisher = publisher;
		this.sleuthBaggageProperties = sleuthBaggageProperties;
		this.entry.set(entry);
	}

	@Override
	public String name() {
		return entry().getKey();
	}

	@Override
	public String get() {
		return this.otelBaggageManager.currentBaggage().getEntryValue(entry().getKey());
	}

	@Override
	public String get(TraceContext traceContext) {
		Entry entry = this.otelBaggageManager.getEntry((OtelTraceContext) traceContext, entry().getKey());
		if (entry == null) {
			return null;
		}
		return entry.getValue();
	}

	@Override
	public void set(String value) {
		doSet(this.currentTraceContext.context(), value);
	}

	private void doSet(TraceContext context, String value) {
		io.opentelemetry.api.baggage.Baggage baggage = Baggage.builder()
				.put(entry().getKey(), value, entry().getEntryMetadata()).build();
		Span current = Span.current();
		if (this.sleuthBaggageProperties.getTagFields().stream().map(String::toLowerCase)
				.anyMatch(s -> s.equals(entry().getKey()))) {
			current.setAttribute(entry().getKey(), value);
		}
		this.publisher.publishEvent(new BaggageChanged(this, baggage, entry().getKey(), value));
		Context withBaggage = Context.current().with(baggage);
		if (context != null) {
			OtelTraceContext traceContext = (OtelTraceContext) context;
			traceContext.addContext(withBaggage);
		}
		Entry previous = entry();
		this.entry.set(Entry.create(previous.getKey(), value, previous.getEntryMetadata()));
		this.contextStorageProvider.get().attach(withBaggage);
	}

	private Entry entry() {
		return this.entry.get();
	}

	@Override
	public void set(TraceContext traceContext, String value) {
		try (CurrentTraceContext.Scope scope = this.currentTraceContext.maybeScope(traceContext)) {
			doSet(traceContext, value);
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
