package gugit.osm.jdbctemplate;

import gugit.om.mapping.WritePacket;

public interface SqlStatementRegistry {

	void register(Class<?> type);
	
	void register(Class<?> type, String insertSQL, String updateSQL);

	String getUpdateSql(WritePacket writePacket);
	
	String getInsertSql(WritePacket writePacket);
	
}
