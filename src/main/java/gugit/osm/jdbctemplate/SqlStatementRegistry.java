package gugit.osm.jdbctemplate;

import gugit.om.mapping.WritePacket;

public interface SqlStatementRegistry {

	void register(Class<?> type);

	String getUpdateSql(WritePacket writePacket);
	
	String getInsertSql(WritePacket writePacket);
	
}
