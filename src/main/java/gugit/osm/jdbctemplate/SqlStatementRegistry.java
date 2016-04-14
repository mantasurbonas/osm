package gugit.osm.jdbctemplate;

import gugit.om.mapping.EntityWritePacket;
import gugit.om.mapping.M2MWritePacket;

public interface SqlStatementRegistry {

	void registerEntity(Class<?> type);

	String getUpdateSql(EntityWritePacket writePacket);
	
	String getInsertSql(EntityWritePacket writePacket);
	
	String[] getWriteSqls(M2MWritePacket writePacket);
	
}
