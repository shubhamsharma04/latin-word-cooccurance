package com.opensam.vo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class PairsVO implements WritableComparable<PairsVO> {

	private Text text;

	public Text getText() {
		return text;
	}

	public void setText(Text text) {
		this.text = text;
	}

	@Override
	public String toString() {
		return text.toString();
	}

	public PairsVO(Text text) {
		super();
		this.text = text;
	}
	
	public PairsVO(){
		text = new Text();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		text.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		text.readFields(in);
	}

	@Override
	public int hashCode() {
		return text.toString().hashCode();
	}

	@Override
	public int compareTo(PairsVO o) {
		int result = -1;
		if (o != null) {
			Text t = o.getText();
			if (t != null) {
				result = this.getText().toString().compareTo(t.toString());
			}
		}
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		PairsVO other = (PairsVO) obj;
		if (text == null) {
			if (other.text != null)
				return false;
		} else if (!text.equals(other.text))
			return false;
		return true;
	}

}
