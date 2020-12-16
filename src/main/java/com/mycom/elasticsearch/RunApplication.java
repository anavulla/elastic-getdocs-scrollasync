package com.mycom.elasticsearch;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.mycom.elasticsearch.service.ElasticSearchService;

/**
 * 
 * @author anavulla
 *
 */
@Component
public class RunApplication {

	@Autowired
	ElasticSearchService elasticSearchService;

	public void getElasticDocs() {

		elasticSearchService.getElasticDocsAndOperate();

	}

}
