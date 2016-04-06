package gugit.osm.jdbctemplate;

import gugit.om.mapping.WritePacket;
import gugit.om.wrapping.EntityMarkingHelper;
import gugit.osm.utils.SQLBuilder;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class SqlStatementRegistryImpl implements SqlStatementRegistry{

	private static class PersistInfo{
		public String insertSQL;
		public String updateSQL;
		public boolean insertsForbidden = false;
		public boolean updatesForbidden = false;
		public PersistInfo(String ins, String upd){
			this.insertSQL=ins;
			this.updateSQL=upd;
		}
	}
	private Map<Class<?>, PersistInfo> registeredPersistInfo = new HashMap<Class<?>, PersistInfo>();

	public synchronized void register(Class<?> type){
		if (registeredPersistInfo.containsKey(type))
			return;
		
		registeredPersistInfo.put(type, new PersistInfo(null, null));
	}
	
	public Collection<Class<?>> getRegisteredTypes(){
		return registeredPersistInfo.keySet();
	}

	public String getUpdateSql(WritePacket writePacket) {
		Class<?> entityClass = writePacket.getEntity().getClass();
		entityClass = EntityMarkingHelper.getEntityClass(entityClass);
		
		if (!registeredPersistInfo.containsKey(entityClass))
			register(entityClass);
		
		PersistInfo persistInfo = registeredPersistInfo.get(entityClass);
		if (persistInfo.updatesForbidden)
			return null;
		
		if (persistInfo.updateSQL != null)
			return persistInfo.updateSQL;
		
		return SQLBuilder.toUpdateSQL(writePacket);
	}

	public String getInsertSql(WritePacket writePacket) {
		Class<?> entityClass = writePacket.getEntity().getClass();
		entityClass = EntityMarkingHelper.getEntityClass(entityClass);
		
		if (!registeredPersistInfo.containsKey(entityClass))
			register(entityClass);
		
		PersistInfo persistInfo = registeredPersistInfo.get(entityClass);
		if (persistInfo.insertsForbidden)
			return null;
		
		if (persistInfo.insertSQL != null)
			return persistInfo.insertSQL;
		
		return SQLBuilder.toInsertSQL(writePacket);
	}

	public void registerUpdateSql(Class<?> type, String updateSql) {
		registeredPersistInfo.get(type).updateSQL = updateSql;
	}

	public void registerInsertSql(Class<?> type, String insertSql) {
		registeredPersistInfo.get(type).insertSQL = insertSql;
	}

	public void registerNoUpdateSql(Class<?> type) {
		registeredPersistInfo.get(type).updatesForbidden = true;
	}

	public void registerNoInsertSql(Class<?> type) {
		registeredPersistInfo.get(type).insertsForbidden = true;
	}

}
