package org.elasticsearch.plugin.rest;



import org.elasticsearch.common.inject.Module;
import org.elasticsearch.plugins.AbstractPlugin;

import java.util.ArrayList;
import java.util.Collection;
public class CommandRestPlugin extends AbstractPlugin {

	@Override
	public String description() {
		return "Restful pipeline command support plugin for Elasticsearch";
	}

	@Override
	public String name() {
		return "rest-command";
	}
	
	@Override
    public Collection<Class<? extends Module>> modules() {
        Collection<Class<? extends Module>> modules = new ArrayList<Class<? extends Module>>();
        modules.add(CommandRestModule.class);
        return modules;
    }

}
