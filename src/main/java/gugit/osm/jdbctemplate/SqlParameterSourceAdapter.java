package gugit.osm.jdbctemplate;

import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import gugit.om.mapping.IWritePacket;
import gugit.om.mapping.NullWriteValue;
import gugit.om.mapping.WritePacketElement;

public class SqlParameterSourceAdapter implements SqlParameterSource{

	private IWritePacket writePacket;

	public SqlParameterSource wrap(IWritePacket writePacket){
		this.writePacket = writePacket;
		return this;
	}
	
	@Override
	public int getSqlType(String arg0) {
		return TYPE_UNKNOWN;
	}

	@Override
	public String getTypeName(String arg0) {
		return null;
	}

	@Override
	public Object getValue(String arg0) throws IllegalArgumentException {
		WritePacketElement element = writePacket.getByFieldName(arg0);
		return element==null?null:(element.value==NullWriteValue.getInstance()?null:element.value);
	}

	@Override
	public boolean hasValue(String arg0) {
		return writePacket.getByFieldName(arg0) != null;
	}

}
