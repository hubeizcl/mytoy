package com.mytoy.starter.tools.elasticsearch.builder;

import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.function.BiFunction;

/**
 * 使用方法参照test的示例
 */
public class PutMappingRequestBuilder {

    private PutMappingRequest putMappingRequest;

    public static PutMappingRequestBuilder builders() {
        return new PutMappingRequestBuilder();
    }

    public PutMappingRequestBuilder putMappingRequest(PutMappingRequest putMappingRequest) {
        this.putMappingRequest = putMappingRequest;
        return this;
    }

    public boolean putMapping(BiFunction<PutMappingRequest, RequestOptions, Boolean> function) {
        return function.apply(putMappingRequest, RequestOptions.DEFAULT);
    }

    public static class MappingDSLBuilders {

        private Boolean isSeven;

        public static class ContentBuilder {

            private XContentBuilder builder;

            public ContentBuilder() throws IOException {
                this.builder = XContentFactory.jsonBuilder();
            }

            public ContentBuilder startObject() throws IOException {
                this.builder.startObject();
                return this;
            }

            public ContentBuilder startObject(String field) throws IOException {
                this.builder.startObject(field);
                return this;
            }

            public ContentBuilder endObject() throws IOException {
                this.builder.endObject();
                return this;
            }

            public ContentBuilder field(String field, String value) throws IOException {
                this.builder.field(field, value);
                return this;
            }

            public XContentBuilder build() {
                return builder;
            }

        }

        private PutMappingRequest putMappingRequest;

        public PutMappingRequest builder() {
            return this.putMappingRequest;
        }

        public MappingDSLBuilders timeout(TimeValue timeout) {
            putMappingRequest.timeout(timeout);
            return this;
        }

        public MappingDSLBuilders masterNodeTimeout(TimeValue masterNodeTimeout) {
            putMappingRequest.masterNodeTimeout(masterNodeTimeout);
            return this;
        }

        public MappingDSLBuilders source(XContentBuilder builder) {
            putMappingRequest.source(builder);
            return this;
        }

        public MappingDSLBuilders indices(String... indices) {
            this.putMappingRequest.indices(indices);
            return this;
        }

        public MappingDSLBuilders types(String types) {
            if (!isSeven) {
                this.putMappingRequest.type(types);
            }
            return this;
        }

        private MappingDSLBuilders() {
            this.putMappingRequest = new PutMappingRequest();
        }

        public static MappingDSLBuilders builders(Boolean isSeven) {
            MappingDSLBuilders mappingDSLBuilders = new MappingDSLBuilders();
            mappingDSLBuilders.isSeven = isSeven;
            return mappingDSLBuilders;
        }
    }


}
