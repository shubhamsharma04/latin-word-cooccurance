package com.opensam.vo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class ExhPayloadVO implements Writable {

	private Text location;

	private IntWritable position;

	private IntWritable nGram;

	private Text valueText;

	private MapWritable valueMap;

	public MapWritable getValueMap() {
		return valueMap;
	}

	public void setValueMap(MapWritable valueMap) {
		this.valueMap = valueMap;
	}

	public Text getValueText() {
		return valueText;
	}

	public void setValueText(Text valueText) {
		this.valueText = valueText;
	}

	public IntWritable getnGram() {
		return nGram;
	}

	public void setnGram(IntWritable nGram) {
		this.nGram = nGram;
	}

	public Text getLocation() {
		return location;
	}

	public void setLocation(Text location) {
		this.location = location;
	}

	public IntWritable getPosition() {
		return position;
	}

	public void setPosition(IntWritable position) {
		this.position = position;
	}

	public ExhPayloadVO() {
		location = new Text();
		position = new IntWritable();
		nGram = new IntWritable();
		valueText = new Text();
		valueMap = new MapWritable();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		location.write(out);
		position.write(out);
		nGram.write(out);
		valueText.write(out);
		valueMap.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		location.readFields(in);
		position.readFields(in);
		nGram.readFields(in);
		valueText.readFields(in);
		valueMap.readFields(in);
	}

	@Override
	public String toString() {
		return "[" + location.toString() + " " + position + "]";
	}

}
