/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.search.aggregations.bucket.geogrid;

import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.index.mapper.GeoPointFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

public abstract class GeoGridAggregatorTestBase<T extends InternalGeoGridBucket> extends AggregatorTestCase {

    private static final String FIELD_NAME = "location";
    private int minPrecision;

    public GeoGridAggregatorTestBase(int minPrecision) {
        this.minPrecision = minPrecision;
    }

    protected abstract int randomPrecision();

    protected abstract String hashAsString(double lng, double lat, int precision);

    public void testNoDocs() throws IOException {
        testCase(new MatchAllDocsQuery(), FIELD_NAME, minPrecision, iw -> {
            // Intentionally not writing any docs
        }, geoGrid -> {
            assertEquals(0, geoGrid.getBuckets().size());
        });
    }

    public void testFieldMissing() throws IOException {
        testCase(new MatchAllDocsQuery(), "wrong_field", minPrecision, iw -> {
            iw.addDocument(Collections.singleton(new LatLonDocValuesField(FIELD_NAME, 10D, 10D)));
        }, geoGrid -> {
            assertEquals(0, geoGrid.getBuckets().size());
        });
    }

    public void testWithSeveralDocs() throws IOException {
        int precision = randomPrecision();
        int numPoints = randomIntBetween(8, 128);
        Map<String, Integer> expectedCountPerHash = new HashMap<>();
        testCase(new MatchAllDocsQuery(), FIELD_NAME, precision, iw -> {
            List<LatLonDocValuesField> points = new ArrayList<>();
            Set<String> distinctHashesPerDoc = new HashSet<>();
            for (int pointId = 0; pointId < numPoints; pointId++) {
                double lat = (180d * randomDouble()) - 90d;
                double lng = (360d * randomDouble()) - 180d;

                // Precision-adjust longitude/latitude to avoid wrong bucket placement
                lng = GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(lng));
                lat = GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(lat));

                points.add(new LatLonDocValuesField(FIELD_NAME, lat, lng));
                String hash = hashAsString(lng, lat, precision);
                if (distinctHashesPerDoc.contains(hash) == false) {
                    expectedCountPerHash.put(hash, expectedCountPerHash.getOrDefault(hash, 0) + 1);
                }
                distinctHashesPerDoc.add(hash);
                if (usually()) {
                    iw.addDocument(points);
                    points.clear();
                    distinctHashesPerDoc.clear();
                }
            }
            if (points.size() != 0) {
                iw.addDocument(points);
            }
        }, geoGrid -> {
            assertEquals(expectedCountPerHash.size(), geoGrid.getBuckets().size());
            for (GeoGrid.Bucket bucket : geoGrid.getBuckets()) {
                assertEquals((long) expectedCountPerHash.get(bucket.getKeyAsString()), bucket.getDocCount());
            }
        });
    }

    private void testCase(Query query, String field, int precision, CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
                          Consumer<InternalGeoGrid<T>> verify) throws IOException {
        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
        buildIndex.accept(indexWriter);
        indexWriter.close();

        IndexReader indexReader = DirectoryReader.open(directory);
        IndexSearcher indexSearcher = newSearcher(indexReader, true, true);

        GeoGridAggregationBuilder aggregationBuilder = new GeoHashGridAggregationBuilder("_name").field(field);
        aggregationBuilder.precision(precision);
        MappedFieldType fieldType = new GeoPointFieldMapper.GeoPointFieldType();
        fieldType.setHasDocValues(true);
        fieldType.setName(FIELD_NAME);

        Aggregator aggregator = createAggregator(aggregationBuilder, indexSearcher, fieldType);
        aggregator.preCollection();
        indexSearcher.search(query, aggregator);
        aggregator.postCollection();
        verify.accept((InternalGeoGrid<T>) aggregator.buildAggregation(0L));

        indexReader.close();
        directory.close();
    }
}
