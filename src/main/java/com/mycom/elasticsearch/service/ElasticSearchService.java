package com.mycom.elasticsearch.service;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * 
 * @author anavulla
 *
 */
@Service
public class ElasticSearchService {

	private static final Logger LOGGER = LoggerFactory.getLogger(ElasticSearchService.class);

	@Autowired
	RestHighLevelClient client;

	@Value("${elasticsearch.index_names}")
	private String index_names;

	private String scrollId;

	private SearchHit[] searchHits;

	public void getElasticDocsAndOperate() {
		List<String> indicies = Stream.of(index_names.split(",", -1)).collect(Collectors.toList());
		indicies.parallelStream().forEach((index_name) -> {

			SearchRequest searchRequest = new SearchRequest(index_name);

			QueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("name", "ajay");

			SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
			searchSourceBuilder.query(matchQueryBuilder);
			searchSourceBuilder.sort("timestamp", SortOrder.ASC);
			searchSourceBuilder.size(5000);

			searchRequest.source(searchSourceBuilder);
			searchRequest.scroll(TimeValue.timeValueMinutes(2L));

			SearchResponse searchResponse = null;

			try {
				searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

				searchHits = searchResponse.getHits().getHits();

				LOGGER.info("searchResponse for index " + index_name + " returned Total hits:"
						+ searchResponse.getHits().getTotalHits());

				if (searchResponse.getHits().getTotalHits() == 0) {
					return;
				}

				scrollId = searchResponse.getScrollId();

				long startTime = System.currentTimeMillis();

				while (searchHits != null && searchHits.length > 0) {
					SearchScrollRequest searchScrollRequest = new SearchScrollRequest(scrollId);
					searchScrollRequest.scroll(TimeValue.timeValueMinutes(2L));

					final CountDownLatch countDownLatch = new CountDownLatch(1);

					/*
					 * using scrollAsync to save time for large number of docs
					 */
					client.scrollAsync(searchScrollRequest, RequestOptions.DEFAULT,
							new ActionListener<SearchResponse>() {

								@Override
								public void onResponse(SearchResponse response) {
									countDownLatch.countDown();

									scrollId = response.getScrollId();

									searchHits = response.getHits().getHits();

									LOGGER.info("onResponse scrollSync with hits:" + searchHits.length);

									performOnHits(searchHits);

								}

								@Override
								public void onFailure(Exception e) {
									// TODO Auto-generated method stub

								}
							});
					countDownLatch.await();
				}

				long estimatedTime = System.currentTimeMillis() - startTime;

				LOGGER.info(
						"Time elapsed in retrieving docs:" + TimeUnit.MILLISECONDS.toMinutes(estimatedTime) + " mins.");

				ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
				clearScrollRequest.addScrollId(scrollId);

				client.clearScrollAsync(clearScrollRequest, RequestOptions.DEFAULT,
						new ActionListener<ClearScrollResponse>() {

							@Override
							public void onResponse(ClearScrollResponse response) {
								// TODO Auto-generated method stub

							}

							@Override
							public void onFailure(Exception e) {
								// TODO Auto-generated method stub

							}
						});

				client.close();

			} catch (Exception e) {

			}

		});
	}

	private void performOnHits(SearchHit[] searchHits) {

		JSONParser jsonParser = new JSONParser();

		for (SearchHit hit : searchHits) {
			try {
				JSONObject jsonObject = (JSONObject) jsonParser.parse(hit.getSourceAsString());

				// convert jsonObject to perform additional operations HERE

				LOGGER.info(jsonObject.toJSONString());

			} catch (Exception e) {

			}
		}
	}

}
