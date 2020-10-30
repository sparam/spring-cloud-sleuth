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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.opentelemetry.api.baggage.Baggage;
import io.opentelemetry.api.baggage.Entry;
import io.opentelemetry.api.baggage.EntryMetadata;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.ContextStorageProvider;

import org.springframework.cloud.sleuth.api.BaggageEntry;
import org.springframework.cloud.sleuth.api.BaggageManager;
import org.springframework.cloud.sleuth.api.CurrentTraceContext;
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

	private final ContextStorageProvider contextStorageProvider;

	private final SleuthBaggageProperties sleuthBaggageProperties;

	private final ApplicationEventPublisher publisher;

	public OtelBaggageManager(CurrentTraceContext currentTraceContext, ContextStorageProvider contextStorageProvider,
			SleuthBaggageProperties sleuthBaggageProperties, ApplicationEventPublisher publisher) {
		this.currentTraceContext = currentTraceContext;
		this.contextStorageProvider = contextStorageProvider;
		this.sleuthBaggageProperties = sleuthBaggageProperties;
		this.publisher = publisher;
	}

	public Map<String, String> getAllBaggage() {
		Map<String, String> baggage = new HashMap<>();
		currentBaggage().getEntries().forEach(entry -> baggage.put(entry.getKey(), entry.getValue()));
		return baggage;
	}

	private io.opentelemetry.api.baggage.Baggage currentBaggage() {
		return Baggage.fromContext(Context.current());
	}

	public BaggageEntry getBaggage(String name) {
		io.opentelemetry.api.baggage.Baggage baggage = currentBaggage();
		Entry entry = entryForName(name, baggage);
		if (entry == null) {
			return createBaggage(name);
		}
		return otelBaggage(entry);
	}

	private Entry entryForName(String name, io.opentelemetry.api.baggage.Baggage baggage) {
		return baggage.getEntries().stream().filter(e -> e.getKey().toLowerCase().equals(name.toLowerCase()))
				.findFirst().orElse(null);
	}

	private BaggageEntry otelBaggage(Entry entry) {
		return new OtelBaggageEntry(this.currentTraceContext, this.contextStorageProvider, this.publisher,
				this.sleuthBaggageProperties, entry);
	}

	public BaggageEntry createBaggage(String name) {
		return baggageWithValue(name, "");
	}

	private BaggageEntry baggageWithValue(String name, String value) {
		List<String> remoteFieldsFields = this.sleuthBaggageProperties.getRemoteFields();
		boolean remoteField = remoteFieldsFields.stream().map(String::toLowerCase)
				.anyMatch(s -> s.equals(name.toLowerCase()));
		EntryMetadata entryMetadata = EntryMetadata.create(propagationString(remoteField));
		Entry entry = Entry.create(name, value, entryMetadata);
		return new OtelBaggageEntry(this.currentTraceContext, this.contextStorageProvider, this.publisher,
				this.sleuthBaggageProperties, entry);
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
