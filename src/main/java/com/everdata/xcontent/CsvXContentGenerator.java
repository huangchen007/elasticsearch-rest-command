package com.everdata.xcontent;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentGenerator;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentString;
import org.elasticsearch.common.xcontent.XContentType;

import com.fasterxml.jackson.core.base.GeneratorBase;
import com.fasterxml.jackson.core.io.SerializedString;
import com.fasterxml.jackson.dataformat.csv.CsvGenerator;

public class CsvXContentGenerator implements XContentGenerator {
	
	protected final CsvGenerator generator;
    private boolean writeLineFeedAtEnd;
    private final GeneratorBase base;
    
    public CsvXContentGenerator(CsvGenerator generator) {
        this.generator = generator;
        if (generator instanceof GeneratorBase) {
            base = (GeneratorBase) generator;
        } else {
            base = null;
        }

    }
        
    
    @Override
    public XContentType contentType() {
    	    	
        return null;
    }

    @Override
    public void usePrettyPrint() {
        generator.useDefaultPrettyPrinter();
    }

    @Override
    public void usePrintLineFeedAtEnd() {
        writeLineFeedAtEnd = true;
    }

    @Override
    public void writeStartArray() throws IOException {
        generator.writeStartArray();
    }

    @Override
    public void writeEndArray() throws IOException {
        generator.writeEndArray();
    }

    @Override
    public void writeStartObject() throws IOException {
        generator.writeStartObject();
    }

    @Override
    public void writeEndObject() throws IOException {
        generator.writeEndObject();
    }

    @Override
    public void writeFieldName(String name) throws IOException {
        generator.writeFieldName(name);
    }

    @Override
    public void writeFieldName(XContentString name) throws IOException {
        generator.writeFieldName(name.getValue());
    }

    @Override
    public void writeString(String text) throws IOException {
        generator.writeString(text);
    }

    @Override
    public void writeString(char[] text, int offset, int len) throws IOException {
        generator.writeString(text, offset, len);
    }

    @Override
    public void writeUTF8String(byte[] text, int offset, int length) throws IOException {
        generator.writeUTF8String(text, offset, length);
    }

    @Override
    public void writeBinary(byte[] data, int offset, int len) throws IOException {
        generator.writeBinary(data, offset, len);
    }

    @Override
    public void writeBinary(byte[] data) throws IOException {
        generator.writeBinary(data);
    }

    @Override
    public void writeNumber(int v) throws IOException {
        generator.writeNumber(v);
    }

    @Override
    public void writeNumber(long v) throws IOException {
        generator.writeNumber(v);
    }

    @Override
    public void writeNumber(double d) throws IOException {
        generator.writeNumber(d);
    }

    @Override
    public void writeNumber(float f) throws IOException {
        generator.writeNumber(f);
    }

    @Override
    public void writeBoolean(boolean state) throws IOException {
        generator.writeBoolean(state);
    }

    @Override
    public void writeNull() throws IOException {
        generator.writeNull();
    }

    @Override
    public void writeStringField(String fieldName, String value) throws IOException {
        generator.writeStringField(fieldName, value);
    }

    @Override
    public void writeStringField(XContentString fieldName, String value) throws IOException {
        generator.writeFieldName(fieldName.getValue());
        generator.writeString(value);
    }

    @Override
    public void writeBooleanField(String fieldName, boolean value) throws IOException {
        generator.writeBooleanField(fieldName, value);
    }

    @Override
    public void writeBooleanField(XContentString fieldName, boolean value) throws IOException {
        generator.writeFieldName(fieldName.getValue());
        generator.writeBoolean(value);
    }

    @Override
    public void writeNullField(String fieldName) throws IOException {
        generator.writeNullField(fieldName);
    }

    @Override
    public void writeNullField(XContentString fieldName) throws IOException {
        generator.writeFieldName(fieldName.getValue());
        generator.writeNull();
    }

    @Override
    public void writeNumberField(String fieldName, int value) throws IOException {
        generator.writeNumberField(fieldName, value);
    }

    @Override
    public void writeNumberField(XContentString fieldName, int value) throws IOException {
        generator.writeFieldName(fieldName.getValue());
        generator.writeNumber(value);
    }

    @Override
    public void writeNumberField(String fieldName, long value) throws IOException {
        generator.writeNumberField(fieldName, value);
    }

    @Override
    public void writeNumberField(XContentString fieldName, long value) throws IOException {
        generator.writeFieldName(fieldName.getValue());
        generator.writeNumber(value);
    }

    @Override
    public void writeNumberField(String fieldName, double value) throws IOException {
        generator.writeNumberField(fieldName, value);
    }

    @Override
    public void writeNumberField(XContentString fieldName, double value) throws IOException {
        generator.writeFieldName(fieldName.getValue());
        generator.writeNumber(value);
    }

    @Override
    public void writeNumberField(String fieldName, float value) throws IOException {
        generator.writeNumberField(fieldName, value);
    }

    @Override
    public void writeNumberField(XContentString fieldName, float value) throws IOException {
        generator.writeFieldName(fieldName.getValue());
        generator.writeNumber(value);
    }

    @Override
    public void writeBinaryField(String fieldName, byte[] data) throws IOException {
        generator.writeBinaryField(fieldName, data);
    }

    @Override
    public void writeBinaryField(XContentString fieldName, byte[] value) throws IOException {
        generator.writeFieldName(fieldName.getValue());
        generator.writeBinary(value);
    }

    @Override
    public void writeArrayFieldStart(String fieldName) throws IOException {
        generator.writeArrayFieldStart(fieldName);
    }

    @Override
    public void writeArrayFieldStart(XContentString fieldName) throws IOException {
        generator.writeFieldName(fieldName.getValue());
        generator.writeStartArray();
    }

    @Override
    public void writeObjectFieldStart(String fieldName) throws IOException {
        generator.writeObjectFieldStart(fieldName);
    }

    @Override
    public void writeObjectFieldStart(XContentString fieldName) throws IOException {
        generator.writeFieldName(fieldName.getValue());
        generator.writeStartObject();
    }

    @Override
    public void writeRawField(String fieldName, byte[] content, OutputStream bos) throws IOException {
        generator.writeFieldName(fieldName);
        generator.writeRaw(':');
        flush();
        bos.write(content);
        finishWriteRaw();
    }

    @Override
    public void writeRawField(String fieldName, byte[] content, int offset, int length, OutputStream bos) throws IOException {
        generator.writeFieldName(fieldName);
        generator.writeRaw(':');
        flush();
        bos.write(content, offset, length);
        finishWriteRaw();
    }

    @Override
    public void writeRawField(String fieldName, InputStream content, OutputStream bos) throws IOException {
        generator.writeFieldName(fieldName);
        generator.writeRaw(':');
        flush();
        Streams.copy(content, bos);
        finishWriteRaw();
    }

    @Override
    public final void writeRawField(String fieldName, BytesReference content, OutputStream bos) throws IOException {
        XContentType contentType = XContentFactory.xContentType(content);
        if (contentType != null) {
            writeObjectRaw(fieldName, content, bos);
        } else {
            writeFieldName(fieldName);
            // we could potentially optimize this to not rely on exception logic...
            String sValue = content.toUtf8();
            try {
                writeNumber(Long.parseLong(sValue));
            } catch (NumberFormatException e) {
                try {
                    writeNumber(Double.parseDouble(sValue));
                } catch (NumberFormatException e1) {
                    writeString(sValue);
                }
            }
        }
    }

    protected void writeObjectRaw(String fieldName, BytesReference content, OutputStream bos) throws IOException {
        generator.writeFieldName(fieldName);
        generator.writeRaw(':');
        flush();
        content.writeTo(bos);
        finishWriteRaw();
    }

    private void finishWriteRaw() {
        assert base != null : "CsvGenerator should be of instance GeneratorBase but was: " + generator.getClass();
        if (base != null) {
            base.getOutputContext().writeValue();
        }
    }

    @Override
    public void copyCurrentStructure(XContentParser parser) throws IOException {
    	
    	throw new IOException("copyCurrentStructure unsupport method，really need this method?");
        // the start of the parser
    	/*
        if (parser.currentToken() == null) {
            parser.nextToken();
        }
        if (parser instanceof CsvXContentParser) {
            generator.copyCurrentStructure(((CsvXContentParser) parser).parser);
        } else {
            XContentHelper.copyCurrentStructure(this, parser);
        }
        */
    }

    @Override
    public void flush() throws IOException {
        generator.flush();
    }

    @Override
    public void close() throws IOException {
        if (generator.isClosed()) {
            return;
        }
        if (writeLineFeedAtEnd) {
            flush();
            generator.writeRaw(LF);
        }
        generator.close();
    }

    private static final SerializedString LF = new SerializedString("\n");

}
