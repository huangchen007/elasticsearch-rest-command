package com.everdata.xcontent;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentGenerator;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import com.fasterxml.jackson.core.JsonEncoding;

import com.fasterxml.jackson.dataformat.csv.CsvFactory;

public class CsvXContent implements XContent {

	@Override
	public XContentType type() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public byte streamSeparator() {
		return '\n';
	}

	private final static CsvFactory csvFactory;
	public final static CsvXContent csvXContent;
	
	static {
		csvFactory = new CsvFactory();
        
        //csvFactory.configure(CsvGenerator.Feature., true);
        
		csvXContent = new CsvXContent();
    }
	
	
	@Override
	public XContentGenerator createGenerator(OutputStream os)
			throws IOException {
		 return new CsvXContentGenerator(csvFactory.createGenerator(os, JsonEncoding.UTF8));
	}

	@Override
	public XContentGenerator createGenerator(Writer writer) throws IOException {
		return new CsvXContentGenerator(csvFactory.createGenerator(writer));
	}

	@Override
	public XContentParser createParser(String content) throws IOException {
		throw new IOException("createParser unsupport method，really need this method?");
	}

	@Override
	public XContentParser createParser(InputStream is) throws IOException {
		throw new IOException("createParser unsupport method，really need this method?");
	}

	@Override
	public XContentParser createParser(byte[] data) throws IOException {
		throw new IOException("createParser unsupport method，really need this method?");
	}

	@Override
	public XContentParser createParser(byte[] data, int offset, int length)
			throws IOException {
		throw new IOException("createParser unsupport method，really need this method?");
	}

	@Override
	public XContentParser createParser(BytesReference bytes) throws IOException {
		throw new IOException("createParser unsupport method，really need this method?");
	}

	@Override
	public XContentParser createParser(Reader reader) throws IOException {
		throw new IOException("createParser unsupport method，really need this method?");
	}

}
