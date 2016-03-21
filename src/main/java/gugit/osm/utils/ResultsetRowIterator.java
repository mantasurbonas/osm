package gugit.osm.utils;

import gugit.om.utils.IDataIterator;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ResultsetRowIterator implements IDataIterator<Object>{
	public int offset = 0;
	
	private ResultSet resultset;
	private int resultsetWidth;

	public ResultsetRowIterator(ResultSet rs) {
		this.resultset = rs;
		try {
			this.resultsetWidth = rs.getMetaData().getColumnCount();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public Object peek() {
		try {
			return resultset.getObject(offset+1);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public Object peek(int i) {
		try {
			return resultset.getObject(i+1);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
	
	public Object getNext(){
		Object res = peek();
		next();
		return res;
	}
	
	public void next() {
		offset ++;
	}

	public void reset() {
		offset = 0;
	}

	public int length() {
		return resultsetWidth;
	}

	public boolean isFinished() {
		return offset == resultsetWidth;
	}

	public void setData(Object[] array) {
		reset();
	}

	public int getPosition() {
		return offset;
	}

	public boolean isOutOfBounds(int position) {
		return position<0 || position>=resultsetWidth;
	}

	
}
