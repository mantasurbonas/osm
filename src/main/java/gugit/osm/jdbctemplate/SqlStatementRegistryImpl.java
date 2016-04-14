package gugit.osm.jdbctemplate;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import gugit.om.mapping.EntityWritePacket;
import gugit.om.mapping.M2MWritePacket;
import gugit.om.wrapping.EntityMarkingHelper;
import gugit.osm.utils.SQLBuilder;

public class SqlStatementRegistryImpl implements SqlStatementRegistry{

	private static class EntityPersistInfo{
		public String insertSQL;
		public String updateSQL;
		public boolean insertsForbidden = false;
		public boolean updatesForbidden = false;
		public EntityPersistInfo(String ins, String upd){
			this.insertSQL=ins;
			this.updateSQL=upd;
		}
	}
	private Map<Class<?>, EntityPersistInfo> registeredEntityPersistInfo = new HashMap<Class<?>, EntityPersistInfo>();
	
	private static class M2MPersistInfo{
		public String[] writeSQLs;
		public boolean writesForbidden = false;
		public M2MPersistInfo(String[] sqls){
			this.writeSQLs = sqls; 
		}
	}
	
	private Map<String, M2MPersistInfo> registeredM2MPersistInfo = new HashMap<String, M2MPersistInfo>();

	public synchronized void registerEntity(Class<?> type){
		if (registeredEntityPersistInfo.containsKey(type))
			return;
		
		registeredEntityPersistInfo.put(type, new EntityPersistInfo(null, null));
	}
	
	public synchronized void registerM2M(String tablename){
		if (registeredM2MPersistInfo.containsKey(tablename))
			return;
		
		registeredM2MPersistInfo.put(tablename, new M2MPersistInfo(null));
	}
	
	public Collection<Class<?>> getRegisteredEntityTypes(){
		return registeredEntityPersistInfo.keySet();
	}

	public Collection<String> getRegisteredM2MTypes(){
		return registeredM2MPersistInfo.keySet();
	}
	
	public String getUpdateSql(EntityWritePacket writePacket) {
		Class<?> entityClass = writePacket.getEntity().getClass();
		entityClass = EntityMarkingHelper.getEntityClass(entityClass);
		
		if (!registeredEntityPersistInfo.containsKey(entityClass))
			registerEntity(entityClass);
		
		EntityPersistInfo persistInfo = registeredEntityPersistInfo.get(entityClass);
		if (persistInfo.updatesForbidden)
			return null;
		
		if (persistInfo.updateSQL != null)
			return persistInfo.updateSQL;
		
		return SQLBuilder.toUpdateSQL(writePacket);
	}

	public String getInsertSql(EntityWritePacket writePacket) {
		Class<?> entityClass = writePacket.getEntity().getClass();
		entityClass = EntityMarkingHelper.getEntityClass(entityClass);
		
		if (!registeredEntityPersistInfo.containsKey(entityClass))
			registerEntity(entityClass);
		
		EntityPersistInfo persistInfo = registeredEntityPersistInfo.get(entityClass);
		if (persistInfo.insertsForbidden)
			return null;
		
		if (persistInfo.insertSQL != null)
			return persistInfo.insertSQL;
		
		return SQLBuilder.toInsertSQL(writePacket);
	}

	@Override
	public String[] getWriteSqls(M2MWritePacket writePacket) {
		String tableName = writePacket.getEntityName();
		
		if (!registeredM2MPersistInfo.containsKey(tableName))
			registerM2M(tableName);
		
		M2MPersistInfo persistInfo = registeredM2MPersistInfo.get(tableName);
		
		if (persistInfo.writesForbidden)
			return null;
		
		if (persistInfo.writeSQLs != null)
			return persistInfo.writeSQLs;
		
		return SQLBuilder.toSQL(writePacket);
	}

	
	public void registerUpdateSql(Class<?> type, String updateSql) {
		registeredEntityPersistInfo.get(type).updateSQL = updateSql;
	}

	public void registerInsertSql(Class<?> type, String insertSql) {
		registeredEntityPersistInfo.get(type).insertSQL = insertSql;
	}

	public void registerNoUpdateSql(Class<?> type) {
		registeredEntityPersistInfo.get(type).updatesForbidden = true;
	}

	public void registerNoInsertSql(Class<?> type) {
		registeredEntityPersistInfo.get(type).insertsForbidden = true;
	}

	public void registerWriteSqls(String tableName, String[] writeSQLs){
		registeredM2MPersistInfo.get(tableName).writeSQLs = writeSQLs;
	}
	
	public void registerNoM2MWrites(String tableName){
		registeredM2MPersistInfo.get(tableName).writesForbidden = true;
	}
}
