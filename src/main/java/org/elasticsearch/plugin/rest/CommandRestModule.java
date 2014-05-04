package org.elasticsearch.plugin.rest;

import org.elasticsearch.common.inject.AbstractModule;

public class CommandRestModule extends AbstractModule {

	@Override
	protected void configure() {
		bind(CommandRestHandler.class).asEagerSingleton();
		bind(JobRestHandler.class).asEagerSingleton();
	}

}
