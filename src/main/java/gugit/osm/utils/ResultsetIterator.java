package gugit.osm.utils;

import gugit.om.utils.IDataIterator;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ResultsetIterator {

	private ResultSet resultset;
	private ResultsetRowIterator rowIterator;

	public ResultsetIterator(ResultSet rs){
		this.resultset = rs;
		this.rowIterator = new ResultsetRowIterator(rs);
	}

	public boolean hasNext() {
		return rowIterator != null;
	}

	public IDataIterator<Object> getNextRowIterator() {
		try {
			if (! resultset.next())
				return null;
			
			rowIterator.reset();
			return rowIterator;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
}
