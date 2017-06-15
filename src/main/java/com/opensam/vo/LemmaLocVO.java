package com.opensam.vo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class LemmaLocVO implements Writable {

	private Text word;

	private Text docId;

	private IntWritable para;

	private IntWritable line;

	private Text lemma;

	public Text getLemma() {
		return lemma;
	}

	public void setLemma(Text lemma) {
		this.lemma = lemma;
	}

	public Text getWord() {
		return word;
	}

	public void setWord(Text word) {
		this.word = word;
	}

	public Text getDocId() {
		return docId;
	}

	public void setDocId(Text docId) {
		this.docId = docId;
	}

	public IntWritable getPara() {
		return para;
	}

	public void setPara(IntWritable para) {
		this.para = para;
	}

	public IntWritable getLine() {
		return line;
	}

	public void setLine(IntWritable line) {
		this.line = line;
	}

	public LemmaLocVO() {
		word = new Text();
		docId = new Text();
		para = new IntWritable();
		line = new IntWritable();
		lemma = new Text();
	}

	@Override
	public int hashCode() {
		return word.toString().hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		LemmaLocVO other = (LemmaLocVO) obj;
		if (word == null) {
			if (other.word != null)
				return false;
		} else if (!word.toString().equals(other.word.toString()))
			return false;
		return true;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		word.write(out);
		docId.write(out);
		para.write(out);
		line.write(out);
		lemma.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		word.readFields(in);
		docId.readFields(in);
		para.readFields(in);
		line.readFields(in);
		lemma.readFields(in);
	}

	@Override
	public String toString() {
		return "[" + word + " " + docId + " " + para + " " + line + "]";
	}

}
