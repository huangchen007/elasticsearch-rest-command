package com.everdata.command;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import com.everdata.parser.AST_Search;

public class JoinQuery {
	public static class Join {
		String[] fromFields;
		AST_Search searchTree;

		public Join(String[] fields, AST_Search search) {
			this.fromFields = fields;
			this.searchTree = search;
		}
	}
	
	public static Set<String> getFieldsToBeChangeName(Collection<String> first, Collection<String> second, Collection<String> join){
		HashSet<String> toBeChange = new HashSet<String>();
		//全部改名
		//toBeChange.addAll(first);
		toBeChange.addAll(second);
		
		toBeChange.removeAll(join);
		
		return toBeChange;
	}
	
	public static void changeDuplicateFieldName(Set<String> tobeChangeFieldName, SearchHit joinHit){
		for(String oldField: tobeChangeFieldName){
			
			Object obj = joinHit.sourceAsMap().remove(oldField);
			joinHit.sourceAsMap().put(String.format("%s.%s", joinHit.type(),oldField), obj);
			
		}
	}
	
	public static HashMap<String, Map<String, Object>> distinctRow(ArrayList<Map<String, Object>> fromFieldsValue, String[] fields){
		HashMap<String, Map<String, Object>> distinct = new HashMap<String, Map<String, Object>>();
		for(Map<String, Object> row : fromFieldsValue){
			StringBuilder dcKey = new StringBuilder();
			for(String field: fields){
				dcKey.append(row.get(field));
			}
			distinct.put(dcKey.toString(), row);
		}
		return distinct;
	}
	
	

	public static void executeJoin(Join join, int size,
			ArrayList<Map<String, Object>> fromFieldsValue, Client client, ESLogger logger)
			throws CommandException {
				
		
		if(fromFieldsValue.size() == 0)
			throw new CommandException("executeJoin error! fromFieldsValue==null 空的前导结果集！");

		// 生成joinFieldsQuery	
		HashMap<String, Map<String, Object>> dcRow = distinctRow(fromFieldsValue, join.fromFields);
		
		BoolQueryBuilder joinFieldsQuery = QueryBuilders.boolQuery();
		
		if(join.fromFields.length == 1){			
			for(Map<String, Object> row : dcRow.values()){				
				joinFieldsQuery.should(QueryBuilders.termQuery(join.fromFields[0], row.get(join.fromFields[0])));
			}
			
		}else{
			for(Map<String, Object> row : dcRow.values()){
				BoolQueryBuilder rowQuery = QueryBuilders.boolQuery();
				for(int i = 0; i < join.fromFields.length; i++){
					rowQuery.must(QueryBuilders.termQuery(join.fromFields[i], row.get(join.fromFields[i])));
				}
				joinFieldsQuery.should(rowQuery);
			}
		}
		// 生成QueryBuilder
		join.searchTree.setJoinFieldsQuery(joinFieldsQuery);
		QueryBuilder joinQueryBuilder = join.searchTree.getQueryBuilder();
		
		
		// 生成
		String[] indices = Search.parseIndices(join.searchTree, client);
		String[] sourceTypes = Search.parseTypes(join.searchTree);
		
		SearchRequestBuilder joinSearch = client.prepareSearch(indices).setTypes(sourceTypes).setFrom(0).setSize(size).setQuery(joinQueryBuilder);
		
		Search.dumpSearchScript(joinSearch, logger);
		
		SearchHits joinHits = joinSearch.execute().actionGet().getHits();
		
		if(	joinHits.getTotalHits() == 0 ){
			logger.info(String.format("join %s 结果表为空", Arrays.toString(join.fromFields)));
			return;
		}
		
		//search hits To hashmap，并改名
		//join前，求keyset的交集，求出有重名，但是不属于join范围的key
		//将后面的key改名，保证不会在join时被覆盖
		Set<String> needToChangeName = getFieldsToBeChangeName(fromFieldsValue.get(0).keySet(), joinHits.getAt(0).sourceAsMap().keySet(), Arrays.asList(join.fromFields));
		
		HashMap<String, SearchHit> joinMap = new HashMap<String, SearchHit>();
		
		Iterator<SearchHit> iterator = joinHits.iterator();
		while(iterator.hasNext()){
			SearchHit _hit = iterator.next();
			
			StringBuilder key = new StringBuilder();
			for(String field : join.fromFields){
				if( field.equals("_id") )
					key.append(_hit.getId());
				else if(field.equals("_index"))
					key.append(_hit.index());
				else if(field.equals("_type"))
					key.append(_hit.type());
				else
					key.append(_hit.sourceAsMap().get(field));
			}
			
			changeDuplicateFieldName(needToChangeName, _hit);
			joinMap.put(key.toString(), _hit);
		}
				
		
		
		for(Map<String, Object> row : fromFieldsValue){
			StringBuilder key = new StringBuilder();
			for(String field : join.fromFields){
				key.append(row.get(field));
			}
			
			//join动作，求合集
			row.putAll(joinMap.get(key.toString()).sourceAsMap());
		}
		
	}
}
