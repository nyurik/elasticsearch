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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

abstract class InternalGeoGridBucket<B extends InternalGeoGridBucket>
    extends InternalMultiBucketAggregation.InternalBucket
    implements GeoGrid.Bucket, Comparable<InternalGeoGridBucket> {

    protected long geohashAsLong;
    protected long docCount;
    protected InternalAggregations aggregations;

    InternalGeoGridBucket(long geohashAsLong, long docCount, InternalAggregations aggregations) {
        this.docCount = docCount;
        this.aggregations = aggregations;
        this.geohashAsLong = geohashAsLong;
    }

    /**
     * Read from a stream.
     */
    InternalGeoGridBucket(StreamInput in) throws IOException {
        geohashAsLong = in.readLong();
        docCount = in.readVLong();
        aggregations = InternalAggregations.readAggregations(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(geohashAsLong);
        out.writeVLong(docCount);
        aggregations.writeTo(out);
    }

    abstract B buildBucket(InternalGeoGridBucket bucket, long geoHashAsLong, long docCount, InternalAggregations aggregations);


    long geohashAsLong() {
        return geohashAsLong;
    }

    long docCount() {
        return docCount;
    }

    @Override
    public long getDocCount() {
        return docCount;
    }

    @Override
    public Aggregations getAggregations() {
        return aggregations;
    }

    @Override
    public int compareTo(InternalGeoGridBucket other) {
        if (this.geohashAsLong > other.geohashAsLong) {
            return 1;
        }
        if (this.geohashAsLong < other.geohashAsLong) {
            return -1;
        }
        return 0;
    }

    public B reduce(List<B> buckets, InternalAggregation.ReduceContext context) {
        List<InternalAggregations> aggregationsList = new ArrayList<>(buckets.size());
        long docCount = 0;
        for (B bucket : buckets) {
            docCount += bucket.docCount;
            aggregationsList.add(bucket.aggregations);
        }
        final InternalAggregations aggs = InternalAggregations.reduce(aggregationsList, context);
        return buildBucket(this, geohashAsLong, docCount, aggs);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Aggregation.CommonFields.KEY.getPreferredName(), getKeyAsString());
        builder.field(Aggregation.CommonFields.DOC_COUNT.getPreferredName(), docCount);
        aggregations.toXContentInternal(builder, params);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InternalGeoGridBucket bucket = (InternalGeoGridBucket) o;
        return geohashAsLong == bucket.geohashAsLong &&
            docCount == bucket.docCount &&
            Objects.equals(aggregations, bucket.aggregations);
    }

    @Override
    public int hashCode() {
        return Objects.hash(geohashAsLong, docCount, aggregations);
    }

}
