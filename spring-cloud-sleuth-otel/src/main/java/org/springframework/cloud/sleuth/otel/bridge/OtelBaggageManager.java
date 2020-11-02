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

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import io.opentelemetry.api.baggage.Baggage;
import io.opentelemetry.api.baggage.Entry;
import io.opentelemetry.api.baggage.EntryMetadata;
import io.opentelemetry.context.Context;

import org.springframework.cloud.sleuth.api.BaggageInScope;
import org.springframework.cloud.sleuth.api.BaggageManager;
import org.springframework.cloud.sleuth.api.CurrentTraceContext;
import org.springframework.cloud.sleuth.api.TraceContext;
import org.springframework.cloud.sleuth.autoconfig.SleuthBaggageProperties;
import org.springframework.context.ApplicationEventPublisher;

/**
 * OpenTelemetry implementation of a {@link BaggageManager}.
 *
 * @author Marcin Grzejszczak
 * @since 3.0.0
 */
public class OtelBaggageManager {

	private final CurrentTraceContext currentTraceContext;

	private final SleuthBaggageProperties sleuthBaggageProperties;

	private final ApplicationEventPublisher publisher;

	public OtelBaggageManager(CurrentTraceContext currentTraceContext, SleuthBaggageProperties sleuthBaggageProperties,
			ApplicationEventPublisher publisher) {
		this.currentTraceContext = currentTraceContext;
		this.sleuthBaggageProperties = sleuthBaggageProperties;
		this.publisher = publisher;
	}

	public Map<String, String> getAllBaggage() {
		Map<String, String> baggage = new HashMap<>();
		currentBaggage().getEntries().forEach(entry -> baggage.put(entry.getKey(), entry.getValue()));
		return baggage;
	}

	CompositeBaggage currentBaggage() {
		OtelTraceContext traceContext = (OtelTraceContext) currentTraceContext.context();
		Context context = Context.current();
		Deque<Context> stack = new ArrayDeque<>();
		if (traceContext != null) {
			stack.addFirst(traceContext.context());
		}
		stack.addFirst(context);
		return new CompositeBaggage(stack);
	}

	public BaggageInScope getBaggage(String name) {
		Entry entry = getBaggage(name, currentBaggage());
		return createNewEntryIfMissing(name, entry);
	}

	protected BaggageInScope createNewEntryIfMissing(String name, Entry entry) {
		if (entry == null) {
			return createBaggage(name);
		}
		return otelBaggage(entry);
	}

	private Entry getBaggage(String name, io.opentelemetry.api.baggage.Baggage baggage) {
		return entryForName(name, baggage);
	}

	public BaggageInScope getBaggage(TraceContext traceContext, String name) {
		OtelTraceContext context = (OtelTraceContext) traceContext;
		// TODO: Refactor
		Deque<Context> stack = new ArrayDeque<>();
		stack.addFirst(Context.current());
		stack.addFirst(context.context());
		Context ctx = removeFirst(stack);
		Entry entry = null;
		while (ctx != null && entry == null) {
			entry = getBaggage(name, Baggage.fromContext(ctx));
			ctx = removeFirst(stack);
		}
		return createNewEntryIfMissing(name, entry);
	}

	protected Entry getEntry(OtelTraceContext traceContext, String name) {
		OtelTraceContext context = traceContext;
		Context ctx = context.context();
		return getBaggage(name, Baggage.fromContext(ctx));
	}

	protected Context removeFirst(Deque<Context> stack) {
		return stack.isEmpty() ? null : stack.removeFirst();
	}

	private Entry entryForName(String name, io.opentelemetry.api.baggage.Baggage baggage) {
		return baggage.getEntries().stream().filter(e -> e.getKey().toLowerCase().equals(name.toLowerCase()))
				.findFirst().orElse(null);
	}

	private BaggageInScope otelBaggage(Entry entry) {
		return new OtelBaggageInScope(this, this.currentTraceContext, this.publisher, this.sleuthBaggageProperties,
				entry);
	}

	public BaggageInScope createBaggage(String name) {
		return createBaggage(name, "");
	}

	public BaggageInScope createBaggage(String name, String value) {
		BaggageInScope baggageInScope = baggageWithValue(name, "");
		return baggageInScope.set(value);
	}

	private BaggageInScope baggageWithValue(String name, String value) {
		List<String> remoteFieldsFields = this.sleuthBaggageProperties.getRemoteFields();
		boolean remoteField = remoteFieldsFields.stream().map(String::toLowerCase)
				.anyMatch(s -> s.equals(name.toLowerCase()));
		EntryMetadata entryMetadata = EntryMetadata.create(propagationString(remoteField));
		Entry entry = Entry.create(name, value, entryMetadata);
		return new OtelBaggageInScope(this, this.currentTraceContext, this.publisher, this.sleuthBaggageProperties,
				entry);
	}

	private String propagationString(boolean remoteField) {
		// TODO: [OTEL] Magic strings
		String propagation = "";
		if (remoteField) {
			propagation = "propagation=unlimited";
		}
		return propagation;
	}

}

class CompositeBaggage implements io.opentelemetry.api.baggage.Baggage {

	private final Deque<Context> stack;

	CompositeBaggage(Deque<Context> stack) {
		this.stack = stack;
	}

	@Override
	public Collection<Entry> getEntries() {
		// parent baggage foo=bar
		// child baggage foo=baz - we want the last one to override the previous one
		Map<String, Entry> map = new HashMap<>();
		Iterator<Context> iterator = this.stack.descendingIterator();
		while (iterator.hasNext()) {
			Context next = iterator.next();
			Baggage baggage = Baggage.fromContext(next);
			Collection<Entry> entries = baggage.getEntries();
			entries.forEach(entry -> map.put(entry.getKey(), entry));
		}
		return map.values();
	}

	@Override
	public String getEntryValue(String entryKey) {
		return getEntries().stream().filter(entry -> entryKey.equals(entry.getKey())).map(Entry::getValue).findFirst()
				.orElse(null);
	}

	@Override
	public Builder toBuilder() {
		return Baggage.builder();
	}

}
