package com.opensam.vo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class NGramPayLoadVO implements Writable {

	private Text author;

	private Text docName;

	private IntWritable chapNo;

	private IntWritable lineNum;

	private IntWritable position;

	public IntWritable getPosition() {
		return position;
	}

	public void setPosition(IntWritable position) {
		this.position = position;
	}

	public NGramPayLoadVO() {
		author = new Text();
		docName = new Text();
		chapNo = new IntWritable();
		lineNum = new IntWritable();
		position = new IntWritable();
	}

	public Text getAuthor() {
		return author;
	}

	public void setAuthor(Text author) {
		this.author = author;
	}

	public Text getDocName() {
		return docName;
	}

	public void setDocName(Text docName) {
		this.docName = docName;
	}

	public IntWritable getChapNo() {
		return chapNo;
	}

	public void setChapNo(IntWritable chapNo) {
		this.chapNo = chapNo;
	}

	public IntWritable getLineNum() {
		return lineNum;
	}

	public void setLineNum(IntWritable lineNum) {
		this.lineNum = lineNum;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		author.write(out);
		docName.write(out);
		chapNo.write(out);
		lineNum.write(out);
		position.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		author.readFields(in);
		docName.readFields(in);
		chapNo.readFields(in);
		lineNum.readFields(in);
		position.readFields(in);
	}

	@Override
	public String toString() {
		return author + "," + docName + "," + chapNo + ","
				+ lineNum + "," + position;
	}

}
